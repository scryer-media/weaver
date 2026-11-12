// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::MemberId;

use super::super::{Unnamed, by_elimination};

// Elimination tried again from the longest file after every file taken,
// each file's fitting members and the files they could be counted afresh,
// with every fit asked counted. `free` is ordered and deduplicated as the
// repair orders it.
fn by_elimination_rescanning(
    free: &[(String, u64)],
    blank: &[(MemberId, Unnamed)],
    asked: &mut u64,
) -> Vec<(String, MemberId)> {
    let mut free = free.to_vec();
    let mut blank = blank.to_vec();
    let mut known = Vec::new();
    let mut fits = |unnamed: Unnamed, long: u64| {
        *asked += 1;
        if unnamed.intact {
            unnamed.length == long
        } else {
            long <= unnamed.length
        }
    };
    while let Some((at, member)) = free.iter().enumerate().find_map(|(at, (_, long))| {
        let fitting: Vec<usize> = (0..blank.len())
            .filter(|candidate| fits(blank[*candidate].1, *long))
            .collect();
        let could_be = free
            .iter()
            .filter(|(_, other)| fitting.iter().any(|c| fits(blank[*c].1, *other)))
            .count();
        let tightest = fitting
            .iter()
            .copied()
            .min_by_key(|candidate| blank[*candidate].1.length)
            .filter(|_| could_be <= fitting.len())?;
        Some((at, tightest))
    }) {
        let (path, _) = free.remove(at);
        known.push((path, blank.swap_remove(member).0));
    }
    known
}

// The free files as the repair hands them to elimination: longest first,
// files of one length in the sets' order, a path repeated next to itself
// once.
fn ordered(mut free: Vec<(String, u64)>) -> Vec<(String, u64)> {
    free.sort_by_key(|(_, long)| std::cmp::Reverse(*long));
    free.dedup();
    free
}

fn blank(length: u64) -> Unnamed {
    Unnamed {
        length,
        intact: false,
        complete: true,
    }
}

fn exact(length: u64) -> Unnamed {
    Unnamed {
        length,
        intact: true,
        complete: true,
    }
}

// A small deterministic generator, so every shape is the same on every run.
struct Seeded(u64);

impl Seeded {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound.max(1)
    }
}

type Shape = (Vec<(String, u64)>, Vec<(MemberId, Unnamed)>);

// Free files and members with no intact byte of the kinds a repair leaves to
// elimination: lengths each their own, lengths shared by several files,
// one path described by more than one set, bounds that reach past others,
// bounds that reach nothing, and nothing at all. A few members known
// exactly are mixed in, which the comparison covers as well.
fn shape(seed: u64) -> Shape {
    let mut random = Seeded(seed);
    let mut free = Vec::new();
    let mut members = Vec::new();
    for group in 0..random.below(8) {
        let long = 1 + random.below(40) * 100;
        for at in 0..1 + random.below(5) {
            free.push((format!("{group}-{at}.bin"), long));
        }
        if random.below(4) == 0 {
            let again = free[random.below(free.len() as u64) as usize].clone();
            free.push(again);
        }
        for _ in 0..random.below(4) {
            members.push(blank(long + random.below(3) * 50));
        }
    }
    for _ in 0..random.below(4) {
        members.push(blank(random.below(45) * 100 + random.below(3)));
    }
    for _ in 0..random.below(2) {
        members.push(exact(1 + random.below(40) * 100));
    }
    // The files arrive in no order of length; the members in the order of
    // their ids, as the repair walks them.
    for at in (1..free.len()).rev() {
        free.swap(at, random.below(at as u64 + 1) as usize);
    }
    for at in (1..members.len()).rev() {
        members.swap(at, random.below(at as u64 + 1) as usize);
    }
    let members = members
        .into_iter()
        .enumerate()
        .map(|(at, unnamed)| (MemberId(at as u32), unnamed))
        .collect();
    (ordered(free), members)
}

// The finding's shape: n distinct descriptions, half short and half long,
// with n - 1 members bounded by the short length and one by the long.
// `spread` gives every description a length of its own.
fn half_short_half_long(n: u32, spread: bool) -> Shape {
    let (short, long) = (1_000u64, 1_000_000u64);
    let step = |at: u32| if spread { u64::from(at) } else { 0 };
    let mut free = Vec::new();
    for at in 0..n / 2 {
        free.push((format!("short-{at}.bin"), short + step(at)));
        free.push((format!("long-{at}.bin"), long + step(at)));
    }
    let mut members: Vec<(MemberId, Unnamed)> = (0..n - 1)
        .map(|at| (MemberId(at), blank(short + step(n))))
        .collect();
    members.push((MemberId(n - 1), blank(long + step(n))));
    (ordered(free), members)
}

// Elimination by margins binds the same members to the same files, in the
// same order, as elimination tried again from the top, and leaves the same
// ones unbound, across many shapes.
#[test]
fn par3_elimination_binds_as_rescanning_does() {
    let mut shapes: Vec<Shape> = (0..20_000).map(shape).collect();
    for n in [2, 3, 4, 8, 33, 64] {
        shapes.push(half_short_half_long(n, false));
        shapes.push(half_short_half_long(n, true));
    }
    let named = |lengths: &[u64]| -> Vec<(String, u64)> {
        let free = lengths
            .iter()
            .enumerate()
            .map(|(at, long)| (format!("{at}.bin"), *long))
            .collect();
        ordered(free)
    };
    let ids = |members: &[Unnamed]| -> Vec<(MemberId, Unnamed)> {
        members
            .iter()
            .enumerate()
            .map(|(at, unnamed)| (MemberId(at as u32), *unnamed))
            .collect()
    };
    // Two files of one length and one member that fits both; a bound
    // between two lengths; members tied on their bound; nothing to match.
    shapes.push((named(&[100, 100]), ids(&[blank(100)])));
    shapes.push((
        named(&[500, 400, 400, 100]),
        ids(&[blank(450), blank(600), blank(400)]),
    ));
    shapes.push((
        named(&[300, 200]),
        ids(&[blank(300), blank(300), blank(250)]),
    ));
    shapes.push((named(&[100, 200]), ids(&[blank(50)])));
    shapes.push((named(&[]), ids(&[blank(50)])));
    shapes.push((named(&[100]), ids(&[])));
    let mut bound = 0;
    for (free, members) in &shapes {
        let (mut asked, mut work) = (0, 0);
        let rescanning = by_elimination_rescanning(free, members, &mut asked);
        let margins = by_elimination(free, members, &mut work);
        assert_eq!(margins, rescanning, "free {free:?} members {members:?}");
        bound += margins.len();
    }
    // The shapes bind members, so the comparison covers bindings and not
    // only files left free.
    assert!(bound > 10_000, "{bound} bindings");
}

// In the finding's shape rescanning asks a number of fits that grows with
// the cube of the files; the margins take a number of steps that grows
// with the files times their logarithm.
#[test]
fn par3_elimination_grows_with_the_files_not_their_cube() {
    let case = |n: u32| {
        let (free, members) = half_short_half_long(n, true);
        let (mut asked, mut work) = (0, 0);
        let rescanning = by_elimination_rescanning(&free, &members, &mut asked);
        let margins = by_elimination(&free, &members, &mut work);
        assert_eq!(margins, rescanning);
        (asked, work)
    };
    let (small_asked, small_work) = case(32);
    let (large_asked, large_work) = case(128);
    // Four times the files: rescanning asks more than thirty-two times as
    // much, the margins take less than eight times the steps.
    assert!(
        large_asked > 32 * small_asked,
        "{small_asked} -> {large_asked}"
    );
    assert!(large_work < 8 * small_work, "{small_work} -> {large_work}");
    // At the finding's size the margins stay a bounded number of steps per
    // file for each doubling of the files.
    for spread in [false, true] {
        let n = 8_192u32;
        let (free, members) = half_short_half_long(n, spread);
        let mut work = 0;
        let margins = by_elimination(&free, &members, &mut work);
        assert!(!margins.is_empty());
        assert!(work < 16 * u64::from(n) * 14, "{work} steps for {n} files");
    }
}
