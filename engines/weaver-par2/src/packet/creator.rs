use crate::error::{Par2Error, Result};

const MAX_CREATOR_BYTES: usize = 100_000;

/// Parsed Creator packet.
///
/// Contains an ASCII string identifying the application that created the PAR2 set.
#[derive(Debug, Clone)]
pub struct CreatorPacket {
    /// The creator application identifier.
    pub creator_id: String,
}

impl CreatorPacket {
    /// Parse a Creator packet from its body (after the 64-byte header).
    pub fn parse(body: &[u8]) -> Result<Self> {
        if body.is_empty() {
            return Err(Par2Error::InvalidCreatorPacket {
                reason: "creator packet is empty".to_string(),
            });
        }
        if body.len() > MAX_CREATOR_BYTES {
            return Err(Par2Error::InvalidCreatorPacket {
                reason: format!("creator payload too large: {} bytes", body.len()),
            });
        }
        // Strip null padding from the end
        let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
        let creator_id = String::from_utf8_lossy(&body[..end]).into_owned();
        Ok(CreatorPacket { creator_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_creator_with_null_padding() {
        let body = b"par2cmdline version 0.8.1\x00\x00\x00";
        let pkt = CreatorPacket::parse(body).unwrap();
        assert_eq!(pkt.creator_id, "par2cmdline version 0.8.1");
    }

    #[test]
    fn parse_creator_no_padding() {
        let body = b"MyApp";
        let pkt = CreatorPacket::parse(body).unwrap();
        assert_eq!(pkt.creator_id, "MyApp");
    }

    #[test]
    fn reject_empty_creator() {
        let err = CreatorPacket::parse(b"").unwrap_err();
        assert!(matches!(err, Par2Error::InvalidCreatorPacket { .. }));
    }
}
