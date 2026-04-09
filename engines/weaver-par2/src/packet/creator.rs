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
    pub fn parse(body: &[u8]) -> Self {
        // Strip null padding from the end
        let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
        let creator_id = String::from_utf8_lossy(&body[..end]).into_owned();
        CreatorPacket { creator_id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_creator_with_null_padding() {
        let body = b"par2cmdline version 0.8.1\x00\x00\x00";
        let pkt = CreatorPacket::parse(body);
        assert_eq!(pkt.creator_id, "par2cmdline version 0.8.1");
    }

    #[test]
    fn parse_creator_no_padding() {
        let body = b"MyApp";
        let pkt = CreatorPacket::parse(body);
        assert_eq!(pkt.creator_id, "MyApp");
    }

    #[test]
    fn parse_creator_empty() {
        let pkt = CreatorPacket::parse(b"");
        assert_eq!(pkt.creator_id, "");
    }
}
