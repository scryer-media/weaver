// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::Post;

// A listing: a comment, then `name XXXXXXXX` per entry.
pub fn listing(name: &str, entries: &[(&str, u32)]) -> Post {
    let mut text = String::from("; generated listing\n");
    for (listed, crc) in entries {
        text.push_str(&format!("{listed} {crc:08X}\n"));
    }
    Post::plain(name, text.into_bytes())
}

pub fn crc(bytes: &[u8]) -> u32 {
    par2_rs::checksum::crc32(bytes)
}

// A listing as a checksum tool writes it, under any name.
pub fn listing_text(header: &str, entries: &[(&str, u32)]) -> Vec<u8> {
    let mut text = String::from(header);
    for (listed, crc) in entries {
        text.push_str(&format!("{listed} {crc:08x}\n"));
    }
    text.into_bytes()
}
