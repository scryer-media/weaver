use crate::error::{Par2Error, Result};
use crate::types::FileId;

/// Parsed File Description packet.
#[derive(Debug, Clone)]
pub struct FileDescriptionPacket {
    /// MD5-based file identifier: MD5(hash_16k || length || name).
    pub file_id: FileId,
    /// MD5 of the entire file.
    pub hash_full: [u8; 16],
    /// MD5 of the first 16,384 bytes of the file.
    pub hash_16k: [u8; 16],
    /// File length in bytes.
    pub file_length: u64,
    /// Filename (ASCII/UTF-8, without null terminator).
    pub filename: String,
}

impl FileDescriptionPacket {
    /// Parse a File Description packet from its body (after the 64-byte header).
    pub fn parse(body: &[u8]) -> Result<Self> {
        // Minimum body: 16 (file_id) + 16 (hash_full) + 16 (hash_16k) + 8 (length) = 56
        // Plus at least some filename bytes (but we allow empty)
        if body.len() < 56 {
            return Err(Par2Error::InvalidFileDescPacket {
                reason: format!("body too short: {} bytes, need at least 56", body.len()),
            });
        }

        let file_id = FileId::from_bytes(body[0..16].try_into().unwrap());
        let hash_full: [u8; 16] = body[16..32].try_into().unwrap();
        let hash_16k: [u8; 16] = body[32..48].try_into().unwrap();
        let file_length = u64::from_le_bytes(body[48..56].try_into().unwrap());

        // Filename: everything after offset 56, strip null padding
        let name_bytes = &body[56..];
        let name_end = name_bytes.iter().position(|&b| b == 0).unwrap_or(name_bytes.len());
        let filename = String::from_utf8_lossy(&name_bytes[..name_end]).into_owned();

        // Sanitize filename to prevent path traversal attacks.
        let filename = sanitize_filename(&filename);

        Ok(FileDescriptionPacket {
            file_id,
            hash_full,
            hash_16k,
            file_length,
            filename,
        })
    }
}

/// Sanitize a PAR2 filename to prevent path traversal.
///
/// - Strips directory separators (`/`, `\`) — PAR2 filenames should be bare names
/// - Removes `..` components
/// - Rejects absolute paths (leading `/` or Windows drive letters)
///
/// If the filename is entirely invalid (e.g., `../../..`), returns an empty string
/// which will cause the file to be skipped during verification/repair.
fn sanitize_filename(name: &str) -> String {
    use std::path::Path;

    if name.is_empty() {
        return String::new();
    }

    // Normalize backslashes to forward slashes for consistent handling.
    let normalized = name.replace('\\', "/");

    // Take only the final path component (strip any directory structure).
    let base = Path::new(&normalized)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    // Reject reserved names that could still be dangerous.
    if base == "." || base == ".." || base.is_empty() {
        return String::new();
    }

    base.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_file_desc_body(
        file_id: [u8; 16],
        hash_full: [u8; 16],
        hash_16k: [u8; 16],
        file_length: u64,
        filename: &[u8],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&file_id);
        body.extend_from_slice(&hash_full);
        body.extend_from_slice(&hash_16k);
        body.extend_from_slice(&file_length.to_le_bytes());
        body.extend_from_slice(filename);
        body
    }

    #[test]
    fn parse_valid_file_desc() {
        let fid = [0x11u8; 16];
        let hf = [0x22u8; 16];
        let h16 = [0x33u8; 16];
        let body = make_file_desc_body(fid, hf, h16, 102400, b"testfile.bin\x00\x00\x00\x00");

        let pkt = FileDescriptionPacket::parse(&body).unwrap();
        assert_eq!(*pkt.file_id.as_bytes(), fid);
        assert_eq!(pkt.hash_full, hf);
        assert_eq!(pkt.hash_16k, h16);
        assert_eq!(pkt.file_length, 102400);
        assert_eq!(pkt.filename, "testfile.bin");
    }

    #[test]
    fn parse_filename_no_null() {
        let body = make_file_desc_body([0; 16], [0; 16], [0; 16], 0, b"noterm");
        let pkt = FileDescriptionPacket::parse(&body).unwrap();
        assert_eq!(pkt.filename, "noterm");
    }

    #[test]
    fn parse_empty_filename() {
        let body = make_file_desc_body([0; 16], [0; 16], [0; 16], 0, b"\x00\x00\x00\x00");
        let pkt = FileDescriptionPacket::parse(&body).unwrap();
        assert_eq!(pkt.filename, "");
    }

    #[test]
    fn reject_too_short() {
        let body = [0u8; 40];
        let err = FileDescriptionPacket::parse(&body).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidFileDescPacket { .. }));
    }

    #[test]
    fn parse_minimum_body() {
        // Exactly 56 bytes: no filename at all
        let body = make_file_desc_body([0; 16], [0; 16], [0; 16], 42, b"");
        let pkt = FileDescriptionPacket::parse(&body).unwrap();
        assert_eq!(pkt.file_length, 42);
        assert_eq!(pkt.filename, "");
    }

    #[test]
    fn sanitize_path_traversal() {
        assert_eq!(sanitize_filename("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_filename(".."), "");
        assert_eq!(sanitize_filename("../.."), "");
        assert_eq!(sanitize_filename("/etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("subdir/file.bin"), "file.bin");
        assert_eq!(sanitize_filename("C:\\Users\\evil\\file.exe"), "file.exe");
        assert_eq!(sanitize_filename("file.bin"), "file.bin");
        assert_eq!(sanitize_filename(""), "");
        assert_eq!(sanitize_filename("."), "");
        assert_eq!(sanitize_filename("normal.par2"), "normal.par2");
    }

    #[test]
    fn parse_path_traversal_filename() {
        let body = make_file_desc_body(
            [0; 16], [0; 16], [0; 16], 100,
            b"../../etc/passwd\x00",
        );
        let pkt = FileDescriptionPacket::parse(&body).unwrap();
        assert_eq!(pkt.filename, "passwd");
    }
}
