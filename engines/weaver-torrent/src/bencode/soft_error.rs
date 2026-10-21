// SPDX-License-Identifier: GPL-3.0-or-later

//! The bencode a strict reader refuses although it decodes: dict keys out
//! of order or repeated, and integers or string lengths with a leading
//! zero (libtorrent `bdecode_node::has_soft_error`).

/// Whether the bencoded value at the start of `buf` holds a soft error.
/// Anything that does not decode counts as one.
pub fn has_soft_error(buf: &[u8]) -> bool {
    value_end(buf, 0).is_none()
}

/// The end of the value starting at `pos`, or `None` on a soft error.
fn value_end(buf: &[u8], pos: usize) -> Option<usize> {
    match *buf.get(pos)? {
        b'i' => {
            if buf.get(pos + 1) == Some(&b'0') && buf.get(pos + 2) != Some(&b'e') {
                return None;
            }
            let len = buf.get(pos + 1..)?.iter().position(|&b| b == b'e')?;
            Some(pos + 1 + len + 1)
        }
        b'l' => {
            let mut at = pos + 1;
            while *buf.get(at)? != b'e' {
                at = value_end(buf, at)?;
            }
            Some(at + 1)
        }
        b'd' => {
            let mut at = pos + 1;
            let mut previous: Option<&[u8]> = None;
            while *buf.get(at)? != b'e' {
                let (key, value) = string_at(buf, at)?;
                if previous.is_some_and(|previous| previous >= key) {
                    return None;
                }
                previous = Some(key);
                at = value_end(buf, value)?;
            }
            Some(at + 1)
        }
        b'0'..=b'9' => string_at(buf, pos).map(|(_, end)| end),
        _ => None,
    }
}

/// The string at `pos` and the position after it.
fn string_at(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    let colon = pos + buf.get(pos..)?.iter().position(|&b| b == b':')?;
    let digits = &buf[pos..colon];
    if digits.is_empty() || (digits[0] == b'0' && digits.len() > 1) {
        return None;
    }
    let len: usize = std::str::from_utf8(digits).ok()?.parse().ok()?;
    let end = colon.checked_add(1)?.checked_add(len)?;
    Some((buf.get(colon + 1..end)?, end))
}

#[cfg(test)]
mod tests {
    use super::has_soft_error;

    #[test]
    fn soft_errors_are_found_as_libtorrent_finds_them() {
        // libtorrent test/test_bdecode.cpp: dict_unordered_keys,
        // dict_duplicate_key, leading_zero_length_prefix, int_leading_zero.
        assert!(has_soft_error(b"d2:abi1e2:aai2ee"));
        assert!(has_soft_error(b"d2:bai1e2:aai2ee"));
        assert!(has_soft_error(b"d2:aai1e1:ai2ee"));
        assert!(!has_soft_error(b"d1:ai1e2:aai2ee"));
        assert!(!has_soft_error(b"d2:aai1e1:bi2ee"));
        assert!(has_soft_error(b"d2:aai1e2:aai2ee"));
        assert!(has_soft_error(b"06:foobar"));
        assert!(!has_soft_error(b"0:"));
        assert!(has_soft_error(b"i01e"));
        // Nested values are checked too.
        assert!(has_soft_error(b"d1:ad1:ci0e1:bi0eee"));
        assert!(has_soft_error(b"l1:ai01ee"));
    }
}
