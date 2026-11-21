// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A set's volumes are fetched by their number in the set once it is known,
// whatever order they were posted in: from their names, or from their
// headers once their first articles arrive. The first article of every
// volume still leads, in posted order. Each row is a sixteen-volume set,
// twice the window, served six articles at a time as six workers land.

use super::harness::{ARTICLE, Outcome, Scenario, payload, run};
use super::rar::posts;
use crate::pipeline::tests::direct_store::{obfuscate_volumes, single_member_store_set};

const VOLUMES: usize = 16;

// The volume number posted at each place.
const SHUFFLED: [usize; VOLUMES] = [9, 3, 14, 0, 7, 12, 5, 1, 15, 10, 2, 8, 13, 4, 11, 6];

fn set() -> Vec<(String, Vec<u8>)> {
    single_member_store_set(
        "ember.bin",
        &payload(VOLUMES * 5 * ARTICLE + 7, 77),
        VOLUMES,
    )
}

// The set posted in `order`, a volume number at each place.
fn posted(volumes: &[(String, Vec<u8>)], order: &[usize]) -> Vec<(String, Vec<u8>)> {
    order
        .iter()
        .map(|number| volumes[*number].clone())
        .collect()
}

async fn served(volumes: Vec<(String, Vec<u8>)>) -> Outcome {
    let outcome = run(&Scenario::new(posts(volumes)).batched(6)).await;
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.files.values().map(Vec::len).sum::<usize>(),
        VOLUMES * 5 * ARTICLE + 7
    );
    outcome
}

// The volume numbers in the order their bodies were first asked for: the
// first article past each volume's first.
fn bodies_by_number(outcome: &Outcome, order: &[usize]) -> Vec<usize> {
    let mut seen = Vec::new();
    for (file, ordinal) in &outcome.order {
        let number = order[*file as usize];
        if *ordinal > 0 && !seen.contains(&number) {
            seen.push(number);
        }
    }
    seen
}

// The posted places in the order the volumes' first articles were asked
// for.
fn heads(outcome: &Outcome) -> Vec<u32> {
    outcome
        .order
        .iter()
        .filter(|(_, ordinal)| *ordinal == 0)
        .map(|(file, _)| *file)
        .collect()
}

#[tokio::test]
async fn a_shuffled_set_is_fetched_by_volume_number() {
    let outcome = served(posted(&set(), &SHUFFLED)).await;
    assert_eq!(heads(&outcome), (0..VOLUMES as u32).collect::<Vec<_>>());
    assert_eq!(
        bodies_by_number(&outcome, &SHUFFLED),
        (0..VOLUMES).collect::<Vec<_>>()
    );
}

// Nameless volumes say their number only in their headers: until their
// first articles arrive they wait in posted order, and then they are
// fetched by number.
#[tokio::test]
async fn a_nameless_shuffled_set_is_fetched_by_volume_number_once_its_headers_say_it() {
    let outcome = served(obfuscate_volumes(&posted(&set(), &SHUFFLED))).await;
    assert_eq!(heads(&outcome), (0..VOLUMES as u32).collect::<Vec<_>>());
    assert_eq!(
        bodies_by_number(&outcome, &SHUFFLED),
        (0..VOLUMES).collect::<Vec<_>>()
    );
}

// Fetched by number, the shuffled set holds no more back at any time than
// the set posted in order does.
#[tokio::test]
async fn a_shuffled_set_holds_no_more_than_the_set_posted_in_order() {
    let in_order: Vec<usize> = (0..VOLUMES).collect();
    let sorted = served(posted(&set(), &in_order)).await;
    let shuffled = served(posted(&set(), &SHUFFLED)).await;
    assert!(
        shuffled.graph().held <= sorted.graph().held,
        "shuffled {} sorted {}",
        shuffled.graph().held,
        sorted.graph().held
    );
}
