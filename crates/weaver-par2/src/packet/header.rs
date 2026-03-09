use md5::{Digest, Md5};

use crate::error::{Par2Error, Result};
use crate::types::RecoverySetId;

/// The 8-byte magic sequence that begins every PAR2 packet.
pub const MAGIC: &[u8; 8] = b"PAR2\x00PKT";

/// Minimum packet size: 64-byte header.
pub const HEADER_SIZE: usize = 64;

/// Maximum allowed packet length (1 GiB).
///
/// PAR2 recovery blocks are typically at most a few hundred MiB (the largest
/// real-world slice sizes). Anything claiming to be larger than 1 GiB is almost
/// certainly malicious or corrupted. This prevents OOM from crafted length fields.
pub const MAX_PACKET_LENGTH: u64 = 1 << 30; // 1 GiB

/// 16-byte packet type signatures.
pub const TYPE_MAIN: &[u8; 16] = b"PAR 2.0\x00Main\x00\x00\x00\x00";
pub const TYPE_FILE_DESC: &[u8; 16] = b"PAR 2.0\x00FileDesc";
pub const TYPE_IFSC: &[u8; 16] = b"PAR 2.0\x00IFSC\x00\x00\x00\x00";
pub const TYPE_RECOVERY: &[u8; 16] = b"PAR 2.0\x00RecvSlic";
pub const TYPE_CREATOR: &[u8; 16] = b"PAR 2.0\x00Creator\x00";

/// The type of a PAR2 packet, determined by the 16-byte type signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Main,
    FileDescription,
    InputFileSliceChecksum,
    RecoverySlice,
    Creator,
    Unknown([u8; 16]),
}

impl PacketType {
    pub fn from_signature(sig: &[u8; 16]) -> Self {
        match sig {
            s if s == TYPE_MAIN => PacketType::Main,
            s if s == TYPE_FILE_DESC => PacketType::FileDescription,
            s if s == TYPE_IFSC => PacketType::InputFileSliceChecksum,
            s if s == TYPE_RECOVERY => PacketType::RecoverySlice,
            s if s == TYPE_CREATOR => PacketType::Creator,
            other => PacketType::Unknown(*other),
        }
    }
}

/// Parsed 64-byte packet header.
#[derive(Debug, Clone)]
pub struct PacketHeader {
    /// Total packet length (header + body), always >= 64 and multiple of 4.
    pub length: u64,
    /// MD5 of bytes 32..length (recovery_set_id + type + body).
    pub packet_hash: [u8; 16],
    /// Recovery set this packet belongs to.
    pub recovery_set_id: RecoverySetId,
    /// The packet type.
    pub packet_type: PacketType,
}

impl PacketHeader {
    /// Parse a 64-byte header from raw bytes.
    ///
    /// `offset` is used only for error reporting (position in file/stream).
    /// This does NOT validate the packet hash (needs the full packet body).
    pub fn parse(data: &[u8], offset: u64) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(Par2Error::PacketTooShort {
                expected: HEADER_SIZE as u64,
                actual: data.len() as u64,
            });
        }

        // Validate magic
        if &data[0..8] != MAGIC {
            return Err(Par2Error::InvalidMagic { offset });
        }

        // Parse length
        let length = u64::from_le_bytes(data[8..16].try_into().unwrap());
        if length < HEADER_SIZE as u64 {
            return Err(Par2Error::PacketTooShort {
                expected: HEADER_SIZE as u64,
                actual: length,
            });
        }
        if length % 4 != 0 {
            return Err(Par2Error::InvalidPacketLength { length });
        }
        if length > MAX_PACKET_LENGTH {
            return Err(Par2Error::InvalidPacketLength { length });
        }

        // Extract fields
        let packet_hash: [u8; 16] = data[16..32].try_into().unwrap();
        let recovery_set_id = RecoverySetId::from_bytes(data[32..48].try_into().unwrap());
        let type_sig: [u8; 16] = data[48..64].try_into().unwrap();
        let packet_type = PacketType::from_signature(&type_sig);

        Ok(PacketHeader {
            length,
            packet_hash,
            recovery_set_id,
            packet_type,
        })
    }

    /// Validate the packet hash against the full packet data (bytes 0..length).
    ///
    /// The hash covers bytes 32..length (recovery_set_id + type + body).
    pub fn validate_hash(&self, full_packet: &[u8], offset: u64) -> Result<()> {
        if (full_packet.len() as u64) < self.length {
            return Err(Par2Error::PacketTooShort {
                expected: self.length,
                actual: full_packet.len() as u64,
            });
        }

        let hash_input = &full_packet[32..self.length as usize];
        let computed: [u8; 16] = Md5::digest(hash_input).into();

        if computed != self.packet_hash {
            return Err(Par2Error::PacketHashMismatch { offset });
        }

        Ok(())
    }

    /// Size of the packet body (everything after the 64-byte header).
    pub fn body_length(&self) -> u64 {
        self.length - HEADER_SIZE as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a valid 64-byte header with a zero-length body.
    fn make_header(packet_type: &[u8; 16], body: &[u8]) -> Vec<u8> {
        let length = (HEADER_SIZE + body.len()) as u64;
        let recovery_set_id = [0xAAu8; 16];
        let type_sig = packet_type;

        // Build bytes 32..length for hashing
        let mut hash_input = Vec::new();
        hash_input.extend_from_slice(&recovery_set_id);
        hash_input.extend_from_slice(type_sig);
        hash_input.extend_from_slice(body);

        let packet_hash: [u8; 16] = Md5::digest(&hash_input).into();

        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.extend_from_slice(&length.to_le_bytes());
        data.extend_from_slice(&packet_hash);
        data.extend_from_slice(&recovery_set_id);
        data.extend_from_slice(type_sig);
        data.extend_from_slice(body);
        data
    }

    #[test]
    fn parse_valid_header() {
        let data = make_header(TYPE_MAIN, &[0u8; 0]);
        let header = PacketHeader::parse(&data, 0).unwrap();
        assert_eq!(header.length, HEADER_SIZE as u64);
        assert_eq!(header.packet_type, PacketType::Main);
        assert_eq!(
            header.recovery_set_id,
            RecoverySetId::from_bytes([0xAA; 16])
        );
    }

    #[test]
    fn parse_all_packet_types() {
        for (sig, expected) in [
            (TYPE_MAIN, PacketType::Main),
            (TYPE_FILE_DESC, PacketType::FileDescription),
            (TYPE_IFSC, PacketType::InputFileSliceChecksum),
            (TYPE_RECOVERY, PacketType::RecoverySlice),
            (TYPE_CREATOR, PacketType::Creator),
        ] {
            let data = make_header(sig, &[]);
            let header = PacketHeader::parse(&data, 0).unwrap();
            assert_eq!(header.packet_type, expected);
        }
    }

    #[test]
    fn unknown_packet_type() {
        let custom = b"PAR 2.0\x00CustomXX";
        let data = make_header(custom, &[]);
        let header = PacketHeader::parse(&data, 0).unwrap();
        assert_eq!(header.packet_type, PacketType::Unknown(*custom));
    }

    #[test]
    fn validate_hash_succeeds() {
        let data = make_header(TYPE_CREATOR, b"test body data!!");
        let header = PacketHeader::parse(&data, 0).unwrap();
        header.validate_hash(&data, 0).unwrap();
    }

    #[test]
    fn validate_hash_fails_on_corruption() {
        let mut data = make_header(TYPE_CREATOR, b"test body data!!");
        // Corrupt a body byte
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        let header = PacketHeader::parse(&data, 42).unwrap();
        let err = header.validate_hash(&data, 42).unwrap_err();
        assert!(matches!(err, Par2Error::PacketHashMismatch { offset: 42 }));
    }

    #[test]
    fn reject_bad_magic() {
        let mut data = make_header(TYPE_MAIN, &[]);
        data[0] = b'X';
        let err = PacketHeader::parse(&data, 100).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidMagic { offset: 100 }));
    }

    #[test]
    fn reject_too_short() {
        let data = [0u8; 32];
        let err = PacketHeader::parse(&data, 0).unwrap_err();
        assert!(matches!(err, Par2Error::PacketTooShort { .. }));
    }

    #[test]
    fn reject_non_aligned_length() {
        let mut data = make_header(TYPE_MAIN, &[]);
        // Set length to 65 (not multiple of 4)
        let bad_len = 65u64;
        data[8..16].copy_from_slice(&bad_len.to_le_bytes());
        let err = PacketHeader::parse(&data, 0).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidPacketLength { length: 65 }));
    }

    #[test]
    fn reject_length_too_small() {
        let mut data = make_header(TYPE_MAIN, &[]);
        // Set length to 60 (< 64)
        let bad_len = 60u64;
        data[8..16].copy_from_slice(&bad_len.to_le_bytes());
        let err = PacketHeader::parse(&data, 0).unwrap_err();
        assert!(matches!(err, Par2Error::PacketTooShort { .. }));
    }

    #[test]
    fn body_length_calculation() {
        let body = [0u8; 32];
        let data = make_header(TYPE_MAIN, &body);
        let header = PacketHeader::parse(&data, 0).unwrap();
        assert_eq!(header.body_length(), 32);
    }

    #[test]
    fn reject_oversized_packet() {
        let mut data = make_header(TYPE_MAIN, &[]);
        // Set length to 2 GiB (exceeds MAX_PACKET_LENGTH)
        let huge_len = (2u64 << 30) & !3; // aligned to 4
        data[8..16].copy_from_slice(&huge_len.to_le_bytes());
        let err = PacketHeader::parse(&data, 0).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidPacketLength { .. }));
    }

    #[test]
    fn accept_packet_at_max_length() {
        let mut data = make_header(TYPE_MAIN, &[]);
        // MAX_PACKET_LENGTH is exactly 1 GiB and is a multiple of 4
        data[8..16].copy_from_slice(&MAX_PACKET_LENGTH.to_le_bytes());
        // This should parse the header successfully (even though we don't have
        // the full body data, header parsing should not fail on length).
        let header = PacketHeader::parse(&data, 0).unwrap();
        assert_eq!(header.length, MAX_PACKET_LENGTH);
    }
}
