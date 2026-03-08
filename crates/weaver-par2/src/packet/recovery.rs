use bytes::Bytes;

use crate::error::{Par2Error, Result};
use crate::types::RecoveryExponent;

/// Parsed Recovery Slice packet.
///
/// Contains one recovery block identified by its exponent.
/// The actual Reed-Solomon math is not performed here; we just store the data.
#[derive(Debug, Clone)]
pub struct RecoverySlicePacket {
    /// The exponent identifying this recovery block.
    pub exponent: RecoveryExponent,
    /// The recovery data (length should equal slice_size from the Main packet).
    pub data: Bytes,
}

impl RecoverySlicePacket {
    /// Parse a Recovery Slice packet from its body (after the 64-byte header).
    pub fn parse(body: &[u8]) -> Result<Self> {
        if body.len() < 4 {
            return Err(Par2Error::InvalidRecoveryPacket {
                reason: format!("body too short: {} bytes, need at least 4", body.len()),
            });
        }

        let exponent = u32::from_le_bytes(body[0..4].try_into().unwrap());
        let data = Bytes::copy_from_slice(&body[4..]);

        Ok(RecoverySlicePacket { exponent, data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_recovery() {
        let mut body = Vec::new();
        body.extend_from_slice(&42u32.to_le_bytes());
        body.extend_from_slice(&[0xAB; 128]);

        let pkt = RecoverySlicePacket::parse(&body).unwrap();
        assert_eq!(pkt.exponent, 42);
        assert_eq!(pkt.data.len(), 128);
        assert!(pkt.data.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn parse_recovery_empty_data() {
        let body = 0u32.to_le_bytes();
        let pkt = RecoverySlicePacket::parse(&body).unwrap();
        assert_eq!(pkt.exponent, 0);
        assert_eq!(pkt.data.len(), 0);
    }

    #[test]
    fn reject_too_short() {
        let body = [0u8; 2];
        let err = RecoverySlicePacket::parse(&body).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidRecoveryPacket { .. }));
    }
}
