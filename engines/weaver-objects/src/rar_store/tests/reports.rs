// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a settle says moved, over the rarlab set of 64 stored RAR5 volumes:
// a settle that moves `place` for a volume it did not touch says so by the
// volume, or the check on the store's placement number finds the volume
// answering otherwise.

use super::owner::Owner;
use super::{fixture, no_trouble, store};

// The first `count` volumes of `rar5_stored_sixtyfour`, under their names.
fn set(count: usize) -> Vec<(String, Vec<u8>)> {
    (1..=count)
        .map(|part| {
            let name = format!("rar5_stored_sixtyfour.part{part:02}.rar");
            let bytes = fixture(&format!("extraction_profiles/{name}"));
            (name, bytes)
        })
        .collect()
}

// The 252 000-byte member the set holds: byte n is (31n + n / 97) mod 251.
fn member() -> Vec<u8> {
    (0..252_000usize)
        .map(|n| ((n * 31 + n / 97) % 251) as u8)
        .collect()
}

// The first volume, then the third to the fifth, then the second: the
// second's settle lays the three after it, which it did not touch.
#[test]
fn a_volume_that_lays_the_held_ones_after_it_moves_them() {
    let mut owner = Owner::new(store(), set(64));
    for volume in [0, 2, 3, 4, 1] {
        owner.frames(volume, 1 << 20);
    }
    no_trouble(&owner);
    for volume in 5..64 {
        owner.frames(volume, 1 << 20);
    }
    for volume in 0..64 {
        owner.close(volume);
    }
    no_trouble(&owner);
    assert_eq!(owner.member("stored64.bin").bytes, member());
    assert_eq!(owner.held(), 0);
}
