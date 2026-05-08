use crate::error::{Par2Error, Result};
use crate::path::translate_par2_name_to_relative;
use crate::types::FileId;

const FIXED_BODY_SIZE: usize = 56;
const MAX_FILENAME_BYTES: usize = 100_000;

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
    /// Original PAR2 filename before filesystem-safe translation.
    pub par2_name: String,
}

impl FileDescriptionPacket {
    /// Parse a File Description packet from its body (after the 64-byte header).
    pub fn parse(body: &[u8]) -> Result<Self> {
        // Minimum body: 16 (file_id) + 16 (hash_full) + 16 (hash_16k) + 8 (length) = 56
        // plus at least one filename byte.
        if body.len() <= FIXED_BODY_SIZE {
            return Err(Par2Error::InvalidFileDescPacket {
                reason: format!(
                    "body too short: {} bytes, need more than {FIXED_BODY_SIZE}",
                    body.len()
                ),
            });
        }

        let file_id = FileId::from_bytes(body[0..16].try_into().unwrap());
        let hash_full: [u8; 16] = body[16..32].try_into().unwrap();
        let hash_16k: [u8; 16] = body[32..48].try_into().unwrap();
        let file_length = u64::from_le_bytes(body[48..56].try_into().unwrap());
        if file_length <= 16_384 && hash_16k != hash_full {
            return Err(Par2Error::InvalidFileDescPacket {
                reason: "hash_16k must match hash_full for files <= 16KiB".to_string(),
            });
        }

        // Filename: everything after offset 56, strip null padding
        let name_bytes = &body[56..];
        if name_bytes.len() > MAX_FILENAME_BYTES {
            return Err(Par2Error::InvalidFileDescPacket {
                reason: format!("filename payload too large: {} bytes", name_bytes.len()),
            });
        }
        let name_end = name_bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(name_bytes.len());
        if name_end == 0 {
            return Err(Par2Error::InvalidFileDescPacket {
                reason: "filename is empty".to_string(),
            });
        }
        let par2_name = String::from_utf8_lossy(&name_bytes[..name_end]).into_owned();
        let filename = translate_par2_name_to_relative(&par2_name)?;

        Ok(FileDescriptionPacket {
            file_id,
            hash_full,
            hash_16k,
            file_length,
            filename,
            par2_name,
        })
    }
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
    fn reject_empty_filename() {
        let body = make_file_desc_body([0; 16], [0; 16], [0; 16], 0, b"\x00\x00\x00\x00");
        let err = FileDescriptionPacket::parse(&body).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidFileDescPacket { .. }));
    }

    #[test]
    fn reject_too_short() {
        let body = [0u8; 40];
        let err = FileDescriptionPacket::parse(&body).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidFileDescPacket { .. }));
    }

    #[test]
    fn reject_body_without_filename() {
        // Exactly 56 bytes: no filename at all
        let body = make_file_desc_body([0; 16], [0; 16], [0; 16], 42, b"");
        let err = FileDescriptionPacket::parse(&body).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidFileDescPacket { .. }));
    }

    #[test]
    fn translates_par2_paths_without_flattening() {
        assert_eq!(
            translate_par2_name_to_relative("../../etc/passwd").unwrap(),
            "%2E%2E/%2E%2E/etc/passwd"
        );
        assert_eq!(
            translate_par2_name_to_relative("/etc/passwd").unwrap(),
            "%2F/etc/passwd"
        );
        assert_eq!(
            translate_par2_name_to_relative("subdir/file.bin").unwrap(),
            "subdir/file.bin"
        );
        assert_eq!(
            translate_par2_name_to_relative("C:\\Users\\evil\\file.exe").unwrap(),
            "C%3A/Users/evil/file.exe"
        );
        assert_eq!(
            translate_par2_name_to_relative("normal.par2").unwrap(),
            "normal.par2"
        );
    }

    #[test]
    fn parse_path_traversal_filename() {
        let body = make_file_desc_body([0; 16], [0; 16], [0; 16], 100, b"../../etc/passwd\x00");
        let pkt = FileDescriptionPacket::parse(&body).unwrap();
        assert_eq!(pkt.par2_name, "../../etc/passwd");
        assert_eq!(pkt.filename, "%2E%2E/%2E%2E/etc/passwd");
    }
}
