// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::Clean;

use super::*;

const NAME: &str = "k3x9q0.bin";
const HOLE: usize = 64;
const RUN: usize = 16 * 1024;
const MAGIC: &[u8; 8] = b"PAR3\0PKT";

// The index and one volume of a set over `seed`'s payload.
fn set(seed: u64) -> (Vec<(String, Vec<u8>)>, Vec<u8>) {
    let data = payload(seed, 6000);
    let files = par3_set("fixture.release", &[("payload.bin", &data)], BLOCK, 3);
    let volume = files
        .iter()
        .find(|(name, _)| name.contains(".vol"))
        .map(|(_, bytes)| bytes.clone())
        .expect("a volume");
    let index = files
        .into_iter()
        .filter(|(name, _)| !name.contains(".vol"))
        .collect();
    (index, volume)
}

fn past_hole(volume: &[u8]) -> &[u8] {
    &volume[HOLE..volume.len().min(HOLE + RUN)]
}

fn claims(job: &mut Job<Par3>, bytes: &[u8]) -> bool {
    let clean = Clean::run(HOLE as u64, bytes);
    !job.object.claim(NAME, clean, None).is_empty()
}

// Where the first packet past the hole that the run holds whole starts,
// and its length.
fn whole_packet(run: &[u8]) -> (usize, usize) {
    let mut from = 0;
    loop {
        let at = from
            + run[from..]
                .windows(8)
                .position(|window| window == MAGIC)
                .expect("a whole packet past the hole");
        let length = u64::from_le_bytes(run[at + 24..at + 32].try_into().unwrap()) as usize;
        if at + length <= run.len() {
            return (at, length);
        }
        from = at + 1;
    }
}

#[test]
fn a_volume_of_the_set_with_no_front_is_claimed_by_a_packet_past_its_hole() {
    let (index, volume) = set(61);
    let mut job = job(&index);
    assert!(claims(&mut job, past_hole(&volume)));
}

#[test]
fn a_volume_with_no_front_is_not_claimed_before_its_set_is_known() {
    let (_, volume) = set(61);
    let mut job = job(&[]);
    assert!(!claims(&mut job, past_hole(&volume)));
}

#[test]
fn a_volume_of_another_set_with_no_front_is_not_claimed() {
    let (index, _) = set(61);
    let (_, theirs) = set(62);
    let mut job = job(&index);
    assert!(!claims(&mut job, past_hole(&theirs)));
}

// A packet of the set inside bytes that are no recovery data, with one
// byte of its body changed: its hash fails, so nothing claims it; the
// same packet whole does.
#[test]
fn a_packet_whose_hash_fails_claims_nothing() {
    let (index, volume) = set(61);
    let run = past_hole(&volume);
    let (at, length) = whole_packet(run);
    let mut file = payload(9, 4096);
    file.extend_from_slice(&run[at..at + length]);
    file.extend_from_slice(&payload(10, 2048));
    let mut job = job(&index);
    let mut spoiled = file.clone();
    spoiled[4096 + length - 1] ^= 0x5a;
    assert!(!claims(&mut job, &spoiled), "a packet that fails its hash");
    assert!(claims(&mut job, &file), "the same packet whole");
}

#[test]
fn magic_alone_claims_nothing() {
    let (index, volume) = set(61);
    let run = past_hole(&volume);
    let (at, length) = whole_packet(run);
    let mut job = job(&index);
    let mut file = payload(9, 4096);
    file.extend_from_slice(MAGIC);
    file.extend_from_slice(&payload(11, 512));
    assert!(!claims(&mut job, &file), "magic alone");
    let mut cut = payload(9, 4096);
    cut.extend_from_slice(&run[at..at + length - 1]);
    assert!(!claims(&mut job, &cut), "a packet the run's end cuts");
}
