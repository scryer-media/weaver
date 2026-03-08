/// Metadata extracted from =ybegin and =ypart headers.
#[derive(Debug, Clone)]
pub struct YencMetadata {
    /// Original filename from the `name` field.
    pub name: String,
    /// Total file size in bytes (from `size` field).
    pub size: u64,
    /// Typical encoded line length (from `line` field).
    pub line_length: u32,
    /// Part number (1-based), `None` for single-part articles.
    pub part: Option<u32>,
    /// Total number of parts, if specified.
    pub total: Option<u32>,
    /// Start byte offset in the original file (1-based), multi-part only.
    pub begin: Option<u64>,
    /// End byte offset in the original file (1-based, inclusive), multi-part only.
    pub end: Option<u64>,
}

/// Result of decoding a yEnc article body.
#[derive(Debug)]
pub struct DecodeResult {
    /// Parsed metadata from headers.
    pub metadata: YencMetadata,
    /// Number of decoded bytes written to the output buffer.
    pub bytes_written: usize,
    /// Computed CRC32 of the decoded data.
    pub part_crc: u32,
    /// Expected part CRC32 from `=yend` (`pcrc32` field), if present.
    pub expected_part_crc: Option<u32>,
    /// Expected full-file CRC32 from `=yend` (`crc32` field), if present.
    pub expected_file_crc: Option<u32>,
    /// Whether the part CRC matches the expected value (true if no expected CRC).
    pub crc_valid: bool,
    /// Whether the `=yend` trailer was present. If false, the article may be
    /// incomplete (truncated download).
    pub has_trailer: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_clone() {
        let meta = YencMetadata {
            name: "test.bin".to_string(),
            size: 1024,
            line_length: 128,
            part: Some(1),
            total: Some(10),
            begin: Some(1),
            end: Some(100),
        };
        let cloned = meta.clone();
        assert_eq!(cloned.name, "test.bin");
        assert_eq!(cloned.size, 1024);
        assert_eq!(cloned.part, Some(1));
    }

    #[test]
    fn metadata_debug() {
        let meta = YencMetadata {
            name: "file.dat".to_string(),
            size: 500,
            line_length: 128,
            part: None,
            total: None,
            begin: None,
            end: None,
        };
        let debug = format!("{:?}", meta);
        assert!(debug.contains("file.dat"));
    }
}
