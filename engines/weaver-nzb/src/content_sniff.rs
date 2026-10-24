// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The extension a file's leading bytes identify, for a delivered file whose
// name carries none. Downstream importers walk a delivery by extension, so a
// payload without one is invisible to them however well it plays.
//
// Pure: the caller reads the head of the file, at most [`SNIFF_BYTES`].

// How much of a file the sniff reads.
pub const SNIFF_BYTES: usize = 4096;

const ASF_HEADER_GUID: [u8; 16] = [
    0x30, 0x26, 0xB2, 0x75, 0x8E, 0x66, 0xCF, 0x11, 0xA6, 0xD9, 0x00, 0xAA, 0x00, 0x62, 0xCE, 0x6C,
];

const MPEG_TS_PACKET: usize = 188;

// The extension `head` identifies, or `None` when the bytes name no media
// or container format this table knows.
pub fn sniff_extension(head: &[u8]) -> Option<&'static str> {
    if head.starts_with(&[0x1A, 0x45, 0xDF, 0xA3]) {
        return matroska_doc_type(head);
    }
    if head.len() >= 12 && &head[4..8] == b"ftyp" {
        return Some(iso_brand_extension(&head[8..12]));
    }
    if head.len() >= 12 && head.starts_with(b"RIFF") {
        return match &head[8..12] {
            b"AVI " => Some("avi"),
            b"WAVE" => Some("wav"),
            _ => None,
        };
    }
    if head.starts_with(&ASF_HEADER_GUID) {
        return Some("wmv");
    }
    if head.starts_with(&[0x00, 0x00, 0x01, 0xBA]) {
        return Some("mpg");
    }
    if head.starts_with(b"FLV\x01") {
        return Some("flv");
    }
    if head.starts_with(b"fLaC") {
        return Some("flac");
    }
    if head.starts_with(b"OggS") {
        return Some("ogg");
    }
    if head.starts_with(b"ID3") {
        return Some("mp3");
    }
    if head.len() > MPEG_TS_PACKET && head[0] == 0x47 && head[MPEG_TS_PACKET] == 0x47 {
        return Some("ts");
    }
    if is_mp3_frame_header(head) {
        return Some("mp3");
    }
    None
}

// The EBML header's DocType: `matroska` or `webm`. Any other, or none in
// the bytes read, names nothing.
fn matroska_doc_type(head: &[u8]) -> Option<&'static str> {
    // DocType is element 0x4282 inside the EBML header; its size is a
    // one- to eight-byte variable-length integer.
    let at = head.windows(2).position(|pair| pair == [0x42, 0x82])?;
    let rest = &head[at + 2..];
    let first = *rest.first()?;
    let width = first.leading_zeros() as usize + 1;
    if width > 8 || rest.len() < width {
        return None;
    }
    let mut size = u64::from(first) & (0xFF >> width);
    for byte in &rest[1..width] {
        size = (size << 8) | u64::from(*byte);
    }
    let value = rest.get(width..width + usize::try_from(size).ok()?)?;
    let value = value.strip_suffix(&[0]).unwrap_or(value);
    match value {
        b"matroska" => Some("mkv"),
        b"webm" => Some("webm"),
        _ => None,
    }
}

// The ISO base media file's major brand.
fn iso_brand_extension(brand: &[u8]) -> &'static str {
    match brand {
        b"qt  " => "mov",
        b"M4V " | b"M4VH" | b"M4VP" => "m4v",
        b"M4A " | b"M4B " => "m4a",
        _ => "mp4",
    }
}

// An MPEG audio Layer III frame header: sync, a defined version, a real
// bitrate and sample rate.
fn is_mp3_frame_header(head: &[u8]) -> bool {
    let [0xFF, second, third, ..] = *head else {
        return false;
    };
    let sync = second & 0xE0 == 0xE0;
    let version_defined = (second >> 3) & 0b11 != 0b01;
    let layer_three = (second >> 1) & 0b11 == 0b01;
    let bitrate = third >> 4;
    let sample_rate = (third >> 2) & 0b11;
    sync && version_defined && layer_three && bitrate != 0 && bitrate != 0xF && sample_rate != 0b11
}

#[cfg(test)]
mod tests {
    use super::*;

    // An EBML header carrying `doc_type`, padded out like a real head.
    fn ebml(doc_type: &[u8]) -> Vec<u8> {
        let mut head = vec![0x1A, 0x45, 0xDF, 0xA3, 0xA3];
        head.extend_from_slice(&[0x42, 0x86, 0x81, 0x01]); // EBMLVersion 1
        head.extend_from_slice(&[0x42, 0x82, 0x80 | doc_type.len() as u8]);
        head.extend_from_slice(doc_type);
        head.extend_from_slice(&[0x42, 0x87, 0x81, 0x04]); // DocTypeVersion 4
        head.resize(SNIFF_BYTES, 0);
        head
    }

    fn ftyp(brand: &[u8; 4]) -> Vec<u8> {
        let mut head = vec![0x00, 0x00, 0x00, 0x20];
        head.extend_from_slice(b"ftyp");
        head.extend_from_slice(brand);
        head.extend_from_slice(&[0, 0, 2, 0]);
        head
    }

    fn riff(form: &[u8; 4]) -> Vec<u8> {
        let mut head = b"RIFF".to_vec();
        head.extend_from_slice(&[0x10, 0x20, 0x30, 0x00]);
        head.extend_from_slice(form);
        head.extend_from_slice(b"LIST");
        head
    }

    fn transport_stream() -> Vec<u8> {
        let mut head = vec![0xA5; MPEG_TS_PACKET * 3];
        for packet in 0..3 {
            head[packet * MPEG_TS_PACKET] = 0x47;
        }
        head
    }

    #[test]
    fn each_known_head_names_its_extension() {
        let mut asf = ASF_HEADER_GUID.to_vec();
        asf.extend_from_slice(&[0; 14]);
        let cases: Vec<(Vec<u8>, &str)> = vec![
            (ebml(b"matroska"), "mkv"),
            (ebml(b"webm"), "webm"),
            (ftyp(b"isom"), "mp4"),
            (ftyp(b"mp42"), "mp4"),
            (ftyp(b"M4V "), "m4v"),
            (ftyp(b"qt  "), "mov"),
            (ftyp(b"M4A "), "m4a"),
            (riff(b"AVI "), "avi"),
            (riff(b"WAVE"), "wav"),
            (transport_stream(), "ts"),
            (vec![0x00, 0x00, 0x01, 0xBA, 0x44, 0x00], "mpg"),
            (b"FLV\x01\x05\x00\x00\x00\x09".to_vec(), "flv"),
            (asf, "wmv"),
            (b"fLaC\x00\x00\x00\x22".to_vec(), "flac"),
            (b"OggS\x00\x02".to_vec(), "ogg"),
            (b"ID3\x04\x00\x00".to_vec(), "mp3"),
            // MPEG-1 Layer III, 128 kbit/s, 44.1 kHz.
            (vec![0xFF, 0xFB, 0x90, 0x64, 0x00], "mp3"),
        ];
        for (head, extension) in cases {
            assert_eq!(sniff_extension(&head), Some(extension), "{head:02x?}");
        }
    }

    #[test]
    fn unknown_bytes_name_nothing() {
        for head in [
            Vec::new(),
            b"plain text, nothing more".to_vec(),
            vec![0x00; SNIFF_BYTES],
            b"Rar!\x1A\x07\x01\x00".to_vec(),
            b"PK\x03\x04".to_vec(),
            riff(b"CDXA"),
            ebml(b"other"),
            // EBML with no DocType in the bytes read.
            vec![0x1A, 0x45, 0xDF, 0xA3, 0x80],
            // A transport-stream sync byte with no second packet behind it.
            vec![0x47, 0x40, 0x00, 0x10],
            // Frame sync with a reserved version, a free bitrate, an AAC
            // layer.
            vec![0xFF, 0xEB, 0x90, 0x00],
            vec![0xFF, 0xFB, 0x00, 0x00],
            vec![0xFF, 0xF1, 0x50, 0x80],
        ] {
            assert_eq!(sniff_extension(&head), None, "{head:02x?}");
        }
    }

    #[test]
    fn a_truncated_doc_type_names_nothing() {
        let mut head = ebml(b"matroska");
        head.truncate(14);
        assert_eq!(sniff_extension(&head), None);
    }
}
