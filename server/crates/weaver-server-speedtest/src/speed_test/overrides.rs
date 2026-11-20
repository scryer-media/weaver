// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Connections per leg: largest-remainder shares of `count` by weight across
// the usable legs, then at least one per usable leg, taken from the leg
// holding the most. With fewer connections than usable legs, each still gets
// one.
pub fn weight_shares(weights: &[u8], usable: &[bool], count: u32) -> Vec<u16> {
    let up: Vec<usize> = (0..weights.len()).filter(|i| usable[*i]).collect();
    let mut targets = vec![0u16; weights.len()];
    if up.is_empty() {
        return targets;
    }
    let weight = |i: usize| u32::from(weights[i]);
    let total_weight: u32 = up.iter().map(|&i| weight(i)).sum();
    if total_weight == 0 {
        // Every usable leg has weight zero; share them out evenly.
        for k in 0..(count as usize).max(up.len()) {
            targets[up[k % up.len()]] += 1;
        }
        return targets;
    }
    let mut assigned = 0u32;
    for &i in &up {
        let share = count * weight(i) / total_weight;
        targets[i] = share as u16;
        assigned += share;
    }
    // The connections left go to the largest remainders.
    for &i in remainder_order(weights, &up, count, total_weight)
        .iter()
        .take((count - assigned) as usize)
    {
        targets[i] += 1;
    }
    for &i in &up {
        if targets[i] > 0 {
            continue;
        }
        let donor = up
            .iter()
            .copied()
            .filter(|&d| targets[d] > 1)
            .max_by_key(|&d| {
                (
                    targets[d],
                    std::cmp::Reverse(weight(d)),
                    std::cmp::Reverse(d),
                )
            });
        if let Some(donor) = donor {
            targets[donor] -= 1;
        }
        targets[i] = 1;
    }
    targets
}

// Usable legs by their remainder of `count`'s weight share, largest first; a
// tie goes to the heavier leg, then the earlier one.
fn remainder_order(weights: &[u8], up: &[usize], count: u32, total_weight: u32) -> Vec<usize> {
    let weight = |i: usize| u32::from(weights[i]);
    let mut order = up.to_vec();
    order.sort_by_key(|&i| {
        (
            std::cmp::Reverse((count * weight(i)).checked_rem(total_weight).unwrap_or(0)),
            std::cmp::Reverse(weight(i)),
            i,
        )
    });
    order
}

// The usable leg furthest below its weight share once one more connection
// opens, with ties broken as the shares break them.
pub fn next_leg(weights: &[u8], usable: &[bool], opened: &[u16]) -> Option<usize> {
    let up: Vec<usize> = (0..weights.len()).filter(|i| usable[*i]).collect();
    let count = up.iter().map(|&i| u32::from(opened[i])).sum::<u32>() + 1;
    let targets = weight_shares(weights, usable, count);
    let total_weight: u32 = up.iter().map(|&i| u32::from(weights[i])).sum();
    remainder_order(weights, &up, count, total_weight)
        .into_iter()
        .enumerate()
        .max_by_key(|&(rank, i)| {
            (
                i32::from(targets[i]) - i32::from(opened[i]),
                std::cmp::Reverse(rank),
            )
        })
        .map(|(_, i)| i)
}
