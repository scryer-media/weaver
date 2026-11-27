// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A nameless RAR4 set whose headers are enciphered (`rar -hp`): the engine
// reads a claimant's front under the secret it was offered, asks for the
// volume's last 40 bytes, where its enciphered end record is, and places the
// volume at the number that record states once it is offered it with them.
// Without the secret, or under a wrong one, it reads nothing and asks for
// nothing, as before.

use weaver_graph::{ByteRange, Claim, Clean};

use super::*;

// The rarlab set `rar4_hp_four`: `rar a -m3 -ma4 -hptestpass123 -v11k`.
const SECRET: &str = "testpass123";
// What a store hands on of a volume's front: never past its first 4 KiB.
const FRONT: usize = 4096;
// The signature and the main header, which `-hp` leaves in the clear.
const MAIN: usize = 20;

fn set() -> Vec<Vec<u8>> {
    (1..=4)
        .map(|part| {
            std::fs::read(format!(
                "{}/../../server/crates/weaver-server-core/tests/fixtures/extraction_profiles/rar4_hp_four.part0{part}.rar",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap()
        })
        .collect()
}

// A name that numbers nothing.
fn hashed(volume: usize) -> String {
    format!("{:032x}", 0x5ea1_0000 + volume)
}

// The engine offered each of `secrets` in turn, then holding the first
// volume by a front cut before its member headers, as a store that yields at
// the solid flag hands it on: nothing in it links the others.
fn holding(set: &[Vec<u8>], secrets: &[&str]) -> RarEngine {
    let mut engine = RarEngine::new();
    for secret in secrets {
        engine.offer_password(secret, &mut Vec::new());
    }
    let bytes = &set[0];
    engine.bound(
        VolumeIndex(0),
        &hashed(0),
        &bytes[..MAIN],
        Some(bytes.len() as u64),
    );
    engine
}

// The claim of volume `volume` by its front, with `tail` read past it.
fn claim(engine: &mut RarEngine, set: &[Vec<u8>], volume: usize, tail: &[u8]) -> Claim {
    let bytes = &set[volume];
    let clean = Clean {
        tail,
        ..Clean::start(&bytes[..FRONT])
    };
    engine.claim(&hashed(volume), clean, Some(bytes.len() as u64))
}

fn last(bytes: &[u8]) -> ByteRange {
    let length = bytes.len() as u64;
    ByteRange::new(length - 40, length)
}

fn tail(bytes: &[u8]) -> &[u8] {
    &bytes[bytes.len() - 40..]
}

// Under the right secret the middle volumes and the last one, kin of the
// first by their fronts, ask for their last 40 bytes, and with them are
// placed at the number their end records state.
#[test]
fn an_enciphered_rar4_volume_asks_for_its_end_record_and_is_placed_by_it() {
    let set = set();
    let mut engine = holding(&set, &[SECRET]);
    for volume in [1, 2, 3] {
        assert_eq!(
            claim(&mut engine, &set, volume, &[]),
            Claim::Wants(last(&set[volume])),
            "volume {volume}"
        );
        assert_eq!(
            claim(&mut engine, &set, volume, tail(&set[volume])),
            Claim::Places(vec![VolumeIndex(volume as u32)]),
            "volume {volume}"
        );
    }
}

// Without a secret the fronts read nothing, and under a wrong one neither
// they nor the end records do: nothing is asked for or placed. The right
// secret, offered after the wrong one, then reads them.
#[test]
fn without_the_secret_nothing_is_asked_for_or_placed() {
    let set = set();
    for secrets in [&[][..], &["wrongpass"][..]] {
        let mut engine = holding(&set, secrets);
        for volume in [1, 2, 3] {
            assert_eq!(
                claim(&mut engine, &set, volume, &[]),
                Claim::default(),
                "{secrets:?} volume {volume}"
            );
            assert_eq!(
                claim(&mut engine, &set, volume, tail(&set[volume])),
                Claim::default(),
                "{secrets:?} volume {volume}"
            );
        }
    }
    let mut engine = holding(&set, &["wrongpass", SECRET]);
    assert_eq!(
        claim(&mut engine, &set, 2, tail(&set[2])),
        Claim::Places(vec![VolumeIndex(2)])
    );
}

// A tail of 40 bytes that does not open to an end record, rarlab bytes from
// inside the volume's data, tells nothing: offered with it, the volume is
// neither placed nor asked for again.
#[test]
fn a_tail_that_opens_to_no_end_record_is_not_asked_for_again() {
    let set = set();
    let mut engine = holding(&set, &[SECRET]);
    assert_eq!(
        claim(&mut engine, &set, 2, &[]),
        Claim::Wants(last(&set[2]))
    );
    let inside = &set[2][FRONT + 1_000..FRONT + 1_040];
    assert_eq!(claim(&mut engine, &set, 2, inside), Claim::default());
}
