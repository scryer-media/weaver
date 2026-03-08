use crate::error::{Par2Error, Result};
use crate::types::{FileId, SliceChecksum};

/// Parsed Input File Slice Checksum (IFSC) packet.
///
/// Contains a file ID and an array of CRC32+MD5 checksum pairs, one per slice.
#[derive(Debug, Clone)]
pub struct IfscPacket {
    /// The file this packet describes.
    pub file_id: FileId,
    /// Per-slice checksums: each entry is (CRC32, MD5).
    pub checksums: Vec<SliceChecksum>,
}

/// Size of one checksum entry: 4 bytes CRC32 + 16 bytes MD5.
const CHECKSUM_ENTRY_SIZE: usize = 20;

impl IfscPacket {
    /// Parse an IFSC packet from its body (after the 64-byte header).
    pub fn parse(body: &[u8]) -> Result<Self> {
        if body.len() < 16 {
            return Err(Par2Error::InvalidIfscPacket {
                reason: format!("body too short: {} bytes, need at least 16", body.len()),
            });
        }

        let file_id = FileId::from_bytes(body[0..16].try_into().unwrap());

        let checksum_area = &body[16..];
        if checksum_area.len() % CHECKSUM_ENTRY_SIZE != 0 {
            return Err(Par2Error::InvalidIfscPacket {
                reason: format!(
                    "checksum area length {} is not a multiple of {}",
                    checksum_area.len(),
                    CHECKSUM_ENTRY_SIZE
                ),
            });
        }

        let count = checksum_area.len() / CHECKSUM_ENTRY_SIZE;
        let mut checksums = Vec::with_capacity(count);

        for i in 0..count {
            let offset = i * CHECKSUM_ENTRY_SIZE;
            let crc32 = u32::from_le_bytes(
                checksum_area[offset..offset + 4].try_into().unwrap(),
            );
            let md5: [u8; 16] = checksum_area[offset + 4..offset + 20].try_into().unwrap();
            checksums.push(SliceChecksum { crc32, md5 });
        }

        Ok(IfscPacket { file_id, checksums })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ifsc_body(file_id: [u8; 16], checksums: &[(u32, [u8; 16])]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&file_id);
        for &(crc, md5) in checksums {
            body.extend_from_slice(&crc.to_le_bytes());
            body.extend_from_slice(&md5);
        }
        body
    }

    #[test]
    fn parse_valid_ifsc() {
        let fid = [0xAA; 16];
        let checksums = vec![
            (0x12345678, [0x01; 16]),
            (0xDEADBEEF, [0x02; 16]),
            (0x00000000, [0x03; 16]),
        ];
        let body = make_ifsc_body(fid, &checksums);

        let pkt = IfscPacket::parse(&body).unwrap();
        assert_eq!(*pkt.file_id.as_bytes(), fid);
        assert_eq!(pkt.checksums.len(), 3);
        assert_eq!(pkt.checksums[0].crc32, 0x12345678);
        assert_eq!(pkt.checksums[0].md5, [0x01; 16]);
        assert_eq!(pkt.checksums[1].crc32, 0xDEADBEEF);
        assert_eq!(pkt.checksums[2].crc32, 0x00000000);
    }

    #[test]
    fn parse_ifsc_no_checksums() {
        let body = make_ifsc_body([0; 16], &[]);
        let pkt = IfscPacket::parse(&body).unwrap();
        assert_eq!(pkt.checksums.len(), 0);
    }

    #[test]
    fn reject_too_short() {
        let body = [0u8; 10];
        let err = IfscPacket::parse(&body).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidIfscPacket { .. }));
    }

    #[test]
    fn reject_non_aligned_checksum_area() {
        // 16 bytes file_id + 15 bytes (not a multiple of 20)
        let mut body = vec![0u8; 16];
        body.extend_from_slice(&[0u8; 15]);
        let err = IfscPacket::parse(&body).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidIfscPacket { .. }));
    }
}
