use crate::error::{Par2Error, Result};
use crate::types::{FileId, RecoverySetId};

/// Parsed Main packet.
///
/// Contains the slice size and lists of file IDs for recovery and non-recovery files.
/// The recovery_set_id from the header equals the MD5 of this packet's body.
#[derive(Debug, Clone)]
pub struct MainPacket {
    /// The recovery set ID (from the packet header).
    pub recovery_set_id: RecoverySetId,
    /// Block/slice size in bytes.
    pub slice_size: u64,
    /// File IDs of files in the recovery set.
    pub recovery_file_ids: Vec<FileId>,
    /// File IDs of files NOT in the recovery set.
    pub non_recovery_file_ids: Vec<FileId>,
}

impl MainPacket {
    /// Parse a Main packet from its body bytes (everything after the 64-byte header).
    pub fn parse(body: &[u8], recovery_set_id: RecoverySetId) -> Result<Self> {
        // Minimum body: 8 (slice_size) + 4 (count) = 12
        if body.len() < 12 {
            return Err(Par2Error::InvalidMainPacket {
                reason: format!("body too short: {} bytes, need at least 12", body.len()),
            });
        }

        let slice_size = u64::from_le_bytes(body[0..8].try_into().unwrap());
        if slice_size == 0 {
            return Err(Par2Error::InvalidMainPacket {
                reason: "slice_size is 0".to_string(),
            });
        }
        if slice_size % 4 != 0 {
            return Err(Par2Error::InvalidMainPacket {
                reason: format!("slice_size {slice_size} is not a multiple of 4"),
            });
        }

        let recovery_file_count =
            u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;

        let file_id_area = &body[12..];
        if !file_id_area.len().is_multiple_of(16) {
            return Err(Par2Error::InvalidMainPacket {
                reason: format!(
                    "file ID area length {} is not a multiple of 16",
                    file_id_area.len()
                ),
            });
        }

        let total_ids = file_id_area.len() / 16;
        if recovery_file_count > total_ids {
            return Err(Par2Error::InvalidMainPacket {
                reason: format!(
                    "recovery_file_count ({recovery_file_count}) exceeds total file IDs ({total_ids})"
                ),
            });
        }

        let mut recovery_file_ids = Vec::with_capacity(recovery_file_count);
        for i in 0..recovery_file_count {
            let offset = i * 16;
            let id: [u8; 16] = file_id_area[offset..offset + 16].try_into().unwrap();
            recovery_file_ids.push(FileId::from_bytes(id));
        }

        let non_recovery_count = total_ids - recovery_file_count;
        let mut non_recovery_file_ids = Vec::with_capacity(non_recovery_count);
        for i in recovery_file_count..total_ids {
            let offset = i * 16;
            let id: [u8; 16] = file_id_area[offset..offset + 16].try_into().unwrap();
            non_recovery_file_ids.push(FileId::from_bytes(id));
        }

        Ok(MainPacket {
            recovery_set_id,
            slice_size,
            recovery_file_ids,
            non_recovery_file_ids,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_main_body(slice_size: u64, recovery_ids: &[[u8; 16]], non_recovery_ids: &[[u8; 16]]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&slice_size.to_le_bytes());
        body.extend_from_slice(&(recovery_ids.len() as u32).to_le_bytes());
        for id in recovery_ids {
            body.extend_from_slice(id);
        }
        for id in non_recovery_ids {
            body.extend_from_slice(id);
        }
        body
    }

    #[test]
    fn parse_valid_main() {
        let file_a = [0x01u8; 16];
        let file_b = [0x02u8; 16];
        let file_c = [0x03u8; 16];
        let body = make_main_body(65536, &[file_a, file_b], &[file_c]);
        let rsid = RecoverySetId::from_bytes([0xFF; 16]);

        let pkt = MainPacket::parse(&body, rsid).unwrap();
        assert_eq!(pkt.slice_size, 65536);
        assert_eq!(pkt.recovery_file_ids.len(), 2);
        assert_eq!(pkt.non_recovery_file_ids.len(), 1);
        assert_eq!(*pkt.recovery_file_ids[0].as_bytes(), file_a);
        assert_eq!(*pkt.recovery_file_ids[1].as_bytes(), file_b);
        assert_eq!(*pkt.non_recovery_file_ids[0].as_bytes(), file_c);
    }

    #[test]
    fn parse_main_no_non_recovery() {
        let file_a = [0x01u8; 16];
        let body = make_main_body(1024, &[file_a], &[]);
        let pkt = MainPacket::parse(&body, RecoverySetId::from_bytes([0; 16])).unwrap();
        assert_eq!(pkt.recovery_file_ids.len(), 1);
        assert_eq!(pkt.non_recovery_file_ids.len(), 0);
    }

    #[test]
    fn parse_main_no_files() {
        let body = make_main_body(4096, &[], &[]);
        let pkt = MainPacket::parse(&body, RecoverySetId::from_bytes([0; 16])).unwrap();
        assert_eq!(pkt.recovery_file_ids.len(), 0);
        assert_eq!(pkt.non_recovery_file_ids.len(), 0);
    }

    #[test]
    fn reject_zero_slice_size() {
        let body = make_main_body(0, &[], &[]);
        let err = MainPacket::parse(&body, RecoverySetId::from_bytes([0; 16])).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidMainPacket { .. }));
    }

    #[test]
    fn reject_non_aligned_slice_size() {
        let body = make_main_body(7, &[], &[]);
        let err = MainPacket::parse(&body, RecoverySetId::from_bytes([0; 16])).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidMainPacket { .. }));
    }

    #[test]
    fn reject_body_too_short() {
        let body = [0u8; 8];
        let err = MainPacket::parse(&body, RecoverySetId::from_bytes([0; 16])).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidMainPacket { .. }));
    }

    #[test]
    fn reject_count_exceeds_ids() {
        // Claims 5 recovery file IDs but only provides 2 total
        let mut body = Vec::new();
        body.extend_from_slice(&1024u64.to_le_bytes());
        body.extend_from_slice(&5u32.to_le_bytes());
        body.extend_from_slice(&[0u8; 32]); // 2 file IDs
        let err = MainPacket::parse(&body, RecoverySetId::from_bytes([0; 16])).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidMainPacket { .. }));
    }
}
