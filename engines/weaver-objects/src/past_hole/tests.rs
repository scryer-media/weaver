// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{Packets, Search, Searched, Searches};

// A format of its own: magic, a hash of every byte from the length on, the
// packet's length, and its set.
struct Toy {
    set: u8,
    longest: u64,
}

const MAGIC: &[u8] = b"TOYPKT\0\x01";
const HEADER: usize = 8 + 8 + 8 + 1;

impl Packets for Toy {
    type Hasher = u64;
    const MAGIC: &'static [u8] = MAGIC;
    const HEADER: usize = HEADER;
    const HASHED_FROM: usize = 16;

    fn ours(&self, header: &[u8]) -> Option<u64> {
        let length = u64::from_le_bytes(header[16..24].try_into().ok()?);
        (header[24] == self.set).then_some(length)
    }

    fn longest(&self) -> u64 {
        self.longest
    }

    fn hasher(&self) -> u64 {
        0
    }

    fn update(hasher: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hasher = hasher.wrapping_mul(31).wrapping_add(u64::from(*byte));
        }
    }

    fn passes(hasher: u64, header: &[u8]) -> bool {
        header[8..16] == hasher.to_le_bytes()
    }
}

// A toy packet of `set` with `body`, its hash passing.
fn packet(set: u8, body: &[u8]) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    out.extend_from_slice(&[0; 8]);
    out.extend_from_slice(&((HEADER + body.len()) as u64).to_le_bytes());
    out.push(set);
    out.extend_from_slice(body);
    let mut hash = 0;
    Toy::update(&mut hash, &out[16..]);
    out[8..16].copy_from_slice(&hash.to_le_bytes());
    out
}

// A packet of the object's set whose hash fails is none: the search reads
// past it, finds nothing in it, and finds the packet after it.
#[test]
fn a_packet_whose_hash_fails_is_read_past() {
    let format = Toy {
        set: 7,
        longest: 4096,
    };
    let mut bad = packet(7, &noise(400, 10));
    bad[HEADER + 5] ^= 1;
    let mut bytes = noise(200, 11);
    bytes.extend(&bad);
    assert_eq!(search(&format, 1, &bytes, 50), Searched::More);
    bytes.extend(packet(7, &noise(400, 12)));
    assert_eq!(search(&format, 1, &bytes, 50), Searched::Found);
}

fn noise(len: usize, seed: u8) -> Vec<u8> {
    let mut state = 0x2545_f491_4f6c_dd1du64 ^ u64::from(seed);
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 56) as u8
        })
        .collect()
}

// Feed `bytes` in pieces of `piece`, the first at `at`, until the search
// says other than more.
fn search(format: &Toy, at: u64, bytes: &[u8], piece: usize) -> Searched {
    let mut search = Search::at(at, format.longest);
    for chunk in bytes.chunks(piece) {
        let searched = search.read(chunk, format);
        if searched != Searched::More {
            return searched;
        }
    }
    Searched::More
}

// The bytes start inside a long packet; the next packet starts far past
// any one piece, and is found in pieces of every size, the magic and
// header cut anywhere.
#[test]
fn a_packet_past_the_first_pieces_is_found_however_they_are_cut() {
    let format = Toy {
        set: 7,
        longest: 4096,
    };
    let mut bytes = noise(3000, 1);
    bytes.extend(packet(7, &noise(2000, 2)));
    for piece in [1, 3, 7, 8, 9, 25, 64, 1000, 5000] {
        assert_eq!(
            search(&format, 100, &bytes, piece),
            Searched::Found,
            "{piece}"
        );
    }
}

// A packet of another set is read past, and one of the object's after it
// is found.
#[test]
fn a_packet_of_another_set_is_read_past() {
    let format = Toy {
        set: 7,
        longest: 4096,
    };
    let mut bytes = noise(500, 3);
    bytes.extend(packet(9, &noise(300, 4)));
    assert_eq!(search(&format, 1, &bytes, 64), Searched::More);
    bytes.extend(packet(7, &noise(300, 5)));
    assert_eq!(search(&format, 1, &bytes, 64), Searched::Found);
}

// Past the longest packet with no packet of the object's begun, the
// volume is none of its.
#[test]
fn nothing_within_the_bound_is_never() {
    let format = Toy {
        set: 7,
        longest: 1000,
    };
    let mut bytes = noise(1100, 6);
    bytes.extend(packet(7, &noise(10, 7)));
    assert_eq!(search(&format, 1, &bytes, 100), Searched::Never);
    let bytes = noise(900, 6);
    assert_eq!(search(&format, 1, &bytes, 100), Searched::More);
}

// The searches by name go on where each stands, start again from bytes
// that do not follow, and end once they say.
#[test]
fn a_search_goes_on_only_from_where_it_stands() {
    let format = Toy {
        set: 7,
        longest: 4096,
    };
    let mut bytes = noise(600, 8);
    bytes.extend(packet(7, &noise(100, 9)));
    let mut searches = Searches::default();
    assert_eq!(
        searches.read("v", (50, &bytes[..300]), &format),
        Searched::More
    );
    // Bytes that do not follow start it again, from them.
    assert_eq!(
        searches.read("v", (50, &bytes[..300]), &format),
        Searched::More
    );
    assert_eq!(
        searches.read("v", (350, &bytes[300..]), &format),
        Searched::Found
    );
    assert_eq!(searches.len(), 0);
}
