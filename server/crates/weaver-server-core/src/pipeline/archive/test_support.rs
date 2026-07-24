use weaver_par2::checksum;

pub(super) const RAR5_SIGNATURE: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

pub(super) fn encode_vint(mut value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        result.push(byte);
        if value == 0 {
            return result;
        }
    }
}

pub(super) fn header(
    header_type: u64,
    common_flags: u64,
    type_body: &[u8],
    extra: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_vint(header_type));

    let flags = common_flags | u64::from(!extra.is_empty());
    body.extend_from_slice(&encode_vint(flags));
    if !extra.is_empty() {
        body.extend_from_slice(&encode_vint(extra.len() as u64));
    }
    body.extend_from_slice(type_body);
    body.extend_from_slice(extra);

    let header_size = encode_vint(body.len() as u64);
    let crc = checksum::crc32(&[header_size.as_slice(), body.as_slice()].concat());

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size);
    result.extend_from_slice(&body);
    result
}

pub(super) fn main_header(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
    let mut type_body = encode_vint(archive_flags);
    if let Some(volume_number) = volume_number {
        type_body.extend_from_slice(&encode_vint(volume_number));
    }
    header(1, 0, &type_body, &[])
}

pub(super) fn end_header(more_volumes: bool) -> Vec<u8> {
    header(5, 0, &encode_vint(u64::from(more_volumes)), &[])
}

pub(super) fn file_header(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
) -> Vec<u8> {
    file_header_with_extra(
        filename,
        common_flags_extra,
        data_size,
        unpacked_size,
        data_crc,
        &[],
    )
}

pub(super) fn file_header_with_extra(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    extra: &[u8],
) -> Vec<u8> {
    let mut type_body = encode_vint(u64::from(data_crc.is_some()) * 0x0004);
    type_body.extend_from_slice(&encode_vint(unpacked_size));
    type_body.extend_from_slice(&encode_vint(0o644));
    if let Some(data_crc) = data_crc {
        type_body.extend_from_slice(&data_crc.to_le_bytes());
    }
    type_body.extend_from_slice(&encode_vint(0));
    type_body.extend_from_slice(&encode_vint(1));
    type_body.extend_from_slice(&encode_vint(filename.len() as u64));
    type_body.extend_from_slice(filename.as_bytes());

    let mut file_body = encode_vint(data_size);
    file_body.extend_from_slice(&type_body);
    header(2, 0x0002 | common_flags_extra, &file_body, extra)
}

pub(super) fn extra_record(record_type: u64, body: &[u8]) -> Vec<u8> {
    let type_bytes = encode_vint(record_type);
    let mut data = encode_vint((type_bytes.len() + body.len()) as u64);
    data.extend_from_slice(&type_bytes);
    data.extend_from_slice(body);
    data
}

pub(super) fn file_version_extra(version: u64) -> Vec<u8> {
    let mut body = encode_vint(0);
    body.extend_from_slice(&encode_vint(version));
    extra_record(0x04, &body)
}

pub(super) fn redirection_extra(redirection_type: u64, target: &str) -> Vec<u8> {
    let mut body = encode_vint(redirection_type);
    body.extend_from_slice(&encode_vint(0));
    body.extend_from_slice(&encode_vint(target.len() as u64));
    body.extend_from_slice(target.as_bytes());
    extra_record(0x05, &body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rar5_signature_and_vints_match_canonical_bytes() {
        assert_eq!(RAR5_SIGNATURE, *b"Rar!\x1a\x07\x01\x00");
        assert_eq!(encode_vint(0), [0x00]);
        assert_eq!(encode_vint(127), [0x7f]);
        assert_eq!(encode_vint(128), [0x80, 0x01]);
        assert_eq!(encode_vint(0x3fff), [0xff, 0x7f]);
    }

    #[test]
    fn main_and_end_headers_match_canonical_crc_bytes() {
        assert_eq!(main_header(1, None), hex("532a344503010001"));
        assert_eq!(end_header(false), hex("19b23a3503050000"));
    }

    fn hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16).unwrap();
                let low = (pair[1] as char).to_digit(16).unwrap();
                ((high << 4) | low) as u8
            })
            .collect()
    }
}
