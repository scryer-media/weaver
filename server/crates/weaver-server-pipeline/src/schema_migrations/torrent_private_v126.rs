// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(crate) use weaver_server_migrations::schema_migrations::torrent_private_v126::*;

// A one-file metainfo, private or not, as bencode spells it.
#[cfg(test)]
pub(crate) fn tiny_metainfo(name: &str, private: bool) -> Vec<u8> {
    let mut info = format!(
        "d6:lengthi3e4:name{}:{name}12:piece lengthi16384e6:pieces20:",
        name.len()
    )
    .into_bytes();
    info.extend_from_slice(&[0xab; 20]);
    if private {
        info.extend_from_slice(b"7:privatei1e");
    }
    info.push(b'e');
    let mut bytes = b"d4:info".to_vec();
    bytes.extend(info);
    bytes.push(b'e');
    bytes
}
