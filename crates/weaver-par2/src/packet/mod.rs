pub mod header;
pub mod main;
pub mod file_desc;
pub mod file_verify;
pub mod recovery;
pub mod creator;

use tracing::{debug, trace, warn};

use crate::error::{Par2Error, Result};
use crate::types::RecoverySetId;

pub use header::{PacketHeader, PacketType, HEADER_SIZE, MAGIC};
pub use main::MainPacket;
pub use file_desc::FileDescriptionPacket;
pub use file_verify::IfscPacket;
pub use recovery::RecoverySlicePacket;
pub use creator::CreatorPacket;

/// A parsed PAR2 packet (any type).
#[derive(Debug, Clone)]
pub enum Packet {
    Main(MainPacket),
    FileDescription(FileDescriptionPacket),
    InputFileSliceChecksum(IfscPacket),
    RecoverySlice(RecoverySlicePacket),
    Creator(CreatorPacket),
    Unknown {
        packet_type: [u8; 16],
        body: Vec<u8>,
    },
}

/// Parse a single packet from a byte slice that starts at the packet header.
///
/// Returns the parsed packet and the number of bytes consumed.
/// `offset` is used for error reporting (position in the file/stream).
pub fn parse_packet(data: &[u8], offset: u64) -> Result<(Packet, usize)> {
    let header = PacketHeader::parse(data, offset)?;
    let total_len = header.length as usize;

    if data.len() < total_len {
        return Err(Par2Error::PacketTooShort {
            expected: header.length,
            actual: data.len() as u64,
        });
    }

    // Validate packet hash
    header.validate_hash(&data[..total_len], offset)?;

    let body = &data[HEADER_SIZE..total_len];

    let packet = match header.packet_type {
        PacketType::Main => {
            debug!("parsed Main packet at offset {offset}");
            Packet::Main(MainPacket::parse(body, header.recovery_set_id)?)
        }
        PacketType::FileDescription => {
            debug!("parsed FileDescription packet at offset {offset}");
            Packet::FileDescription(FileDescriptionPacket::parse(body)?)
        }
        PacketType::InputFileSliceChecksum => {
            debug!("parsed IFSC packet at offset {offset}");
            Packet::InputFileSliceChecksum(IfscPacket::parse(body)?)
        }
        PacketType::RecoverySlice => {
            debug!("parsed RecoverySlice packet at offset {offset}");
            Packet::RecoverySlice(RecoverySlicePacket::parse(body)?)
        }
        PacketType::Creator => {
            debug!("parsed Creator packet at offset {offset}");
            Packet::Creator(CreatorPacket::parse(body))
        }
        PacketType::Unknown(sig) => {
            debug!("parsed Unknown packet type at offset {offset}");
            Packet::Unknown {
                packet_type: sig,
                body: body.to_vec(),
            }
        }
    };

    Ok((packet, total_len))
}

/// Scan a byte stream for PAR2 packets.
///
/// Scans through `data` looking for valid PAR2 packets. When a valid packet is
/// found, it is parsed and added to the result. When invalid data is encountered,
/// we scan forward byte-by-byte looking for the next magic sequence.
///
/// `base_offset` is the offset of `data[0]` in the original file (for error reporting).
pub fn scan_packets(data: &[u8], base_offset: u64) -> Vec<(Packet, u64)> {
    let mut packets = Vec::new();
    let mut pos = 0;

    while pos + HEADER_SIZE <= data.len() {
        // Try to parse a packet at the current position
        let offset = base_offset + pos as u64;

        match parse_packet(&data[pos..], offset) {
            Ok((packet, consumed)) => {
                trace!("packet at offset {offset}, size {consumed}");
                packets.push((packet, offset));
                pos += consumed;
            }
            Err(_) => {
                // Scan forward to find the next magic sequence
                match find_next_magic(&data[pos + 1..]) {
                    Some(skip) => {
                        let skipped = skip + 1;
                        warn!(
                            "skipped {skipped} bytes at offset {offset} looking for next packet"
                        );
                        pos += skipped;
                    }
                    None => {
                        // No more magic sequences found
                        break;
                    }
                }
            }
        }
    }

    packets
}

/// Find the byte offset of the next PAR2 magic sequence in `data`.
fn find_next_magic(data: &[u8]) -> Option<usize> {
    if data.len() < MAGIC.len() {
        return None;
    }
    for i in 0..=data.len() - MAGIC.len() {
        if &data[i..i + MAGIC.len()] == MAGIC {
            return Some(i);
        }
    }
    None
}

/// Extract the recovery set ID from the first Main packet found in the data.
pub fn find_recovery_set_id(packets: &[(Packet, u64)]) -> Option<RecoverySetId> {
    for (packet, _) in packets {
        if let Packet::Main(main) = packet {
            return Some(main.recovery_set_id);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use md5::{Digest, Md5};

    /// Helper to build a complete valid packet (header + body).
    fn make_full_packet(packet_type: &[u8; 16], body: &[u8], recovery_set_id: [u8; 16]) -> Vec<u8> {
        let length = (HEADER_SIZE + body.len()) as u64;

        // Build bytes 32..length for hashing
        let mut hash_input = Vec::new();
        hash_input.extend_from_slice(&recovery_set_id);
        hash_input.extend_from_slice(packet_type);
        hash_input.extend_from_slice(body);

        let packet_hash: [u8; 16] = Md5::digest(&hash_input).into();

        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.extend_from_slice(&length.to_le_bytes());
        data.extend_from_slice(&packet_hash);
        data.extend_from_slice(&recovery_set_id);
        data.extend_from_slice(packet_type);
        data.extend_from_slice(body);
        data
    }

    fn make_creator_packet(creator: &str, rsid: [u8; 16]) -> Vec<u8> {
        // Pad creator to multiple of 4 for body alignment
        let mut body = creator.as_bytes().to_vec();
        while body.len() % 4 != 0 {
            body.push(0);
        }
        make_full_packet(header::TYPE_CREATOR, &body, rsid)
    }

    fn make_main_packet_bytes(slice_size: u64, rsid: [u8; 16]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&slice_size.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes()); // 0 recovery file IDs
        make_full_packet(header::TYPE_MAIN, &body, rsid)
    }

    #[test]
    fn parse_creator_packet() {
        let rsid = [0x42; 16];
        let data = make_creator_packet("TestCreator", rsid);
        let (packet, consumed) = parse_packet(&data, 0).unwrap();
        assert_eq!(consumed, data.len());
        match packet {
            Packet::Creator(c) => assert_eq!(c.creator_id, "TestCreator"),
            other => panic!("expected Creator, got {other:?}"),
        }
    }

    #[test]
    fn scan_multiple_packets() {
        let rsid = [0x11; 16];
        let mut stream = Vec::new();
        stream.extend_from_slice(&make_creator_packet("App1", rsid));
        stream.extend_from_slice(&make_main_packet_bytes(4096, rsid));
        stream.extend_from_slice(&make_creator_packet("App2", rsid));

        let packets = scan_packets(&stream, 0);
        assert_eq!(packets.len(), 3);
        assert!(matches!(&packets[0].0, Packet::Creator(_)));
        assert!(matches!(&packets[1].0, Packet::Main(_)));
        assert!(matches!(&packets[2].0, Packet::Creator(_)));
    }

    #[test]
    fn scan_skips_garbage() {
        let rsid = [0x22; 16];
        let mut stream = Vec::new();
        // Some garbage bytes before the first packet
        stream.extend_from_slice(&[0xFF; 37]);
        stream.extend_from_slice(&make_creator_packet("Found", rsid));

        let packets = scan_packets(&stream, 0);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].1, 37); // offset should be 37
        match &packets[0].0 {
            Packet::Creator(c) => assert_eq!(c.creator_id, "Found"),
            other => panic!("expected Creator, got {other:?}"),
        }
    }

    #[test]
    fn scan_handles_garbage_between_packets() {
        let rsid = [0x33; 16];
        let mut stream = Vec::new();
        stream.extend_from_slice(&make_creator_packet("First", rsid));
        // Garbage between packets
        stream.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        stream.extend_from_slice(&make_creator_packet("Second", rsid));

        let packets = scan_packets(&stream, 100);
        assert_eq!(packets.len(), 2);
    }

    #[test]
    fn scan_empty_data() {
        let packets = scan_packets(&[], 0);
        assert!(packets.is_empty());
    }

    #[test]
    fn scan_short_data() {
        let packets = scan_packets(&[0u8; 10], 0);
        assert!(packets.is_empty());
    }

    #[test]
    fn find_next_magic_works() {
        let mut data = vec![0u8; 20];
        data.extend_from_slice(MAGIC);
        data.extend_from_slice(&[0u8; 10]);
        assert_eq!(find_next_magic(&data), Some(20));
    }

    #[test]
    fn find_next_magic_at_start() {
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        assert_eq!(find_next_magic(&data), Some(0));
    }

    #[test]
    fn find_next_magic_not_found() {
        let data = [0u8; 100];
        assert_eq!(find_next_magic(&data), None);
    }

    #[test]
    fn parse_packet_hash_mismatch() {
        let rsid = [0; 16];
        let mut data = make_creator_packet("test", rsid);
        // Corrupt a body byte
        let last = data.len() - 1;
        data[last] ^= 0x01;
        let err = parse_packet(&data, 5).unwrap_err();
        assert!(matches!(err, Par2Error::PacketHashMismatch { offset: 5 }));
    }

    #[test]
    fn parse_unknown_packet_type() {
        let custom_type = b"PAR 2.0\x00TestType";
        let body = [0u8; 16]; // 16 bytes body
        let rsid = [0; 16];
        let data = make_full_packet(custom_type, &body, rsid);

        let (packet, _) = parse_packet(&data, 0).unwrap();
        match packet {
            Packet::Unknown { packet_type, body: b } => {
                assert_eq!(packet_type, *custom_type);
                assert_eq!(b.len(), 16);
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn find_recovery_set_id_works() {
        let rsid = [0x77; 16];
        let stream = make_main_packet_bytes(1024, rsid);
        let packets = scan_packets(&stream, 0);
        let found = find_recovery_set_id(&packets).unwrap();
        assert_eq!(*found.as_bytes(), rsid);
    }

    #[test]
    fn find_recovery_set_id_none() {
        let rsid = [0; 16];
        let stream = make_creator_packet("test", rsid);
        let packets = scan_packets(&stream, 0);
        assert!(find_recovery_set_id(&packets).is_none());
    }
}
