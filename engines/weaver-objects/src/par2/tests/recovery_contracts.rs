// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn verified_content_outvotes_a_swapped_damaged_name() {
    let a = payload(101, 259);
    let b = payload(102, 259);
    let files = par2_set("identity", &[("a.bin", &a), ("b.bin", &b)], 64, 2, 1);
    for (restore, reverse, first_name) in [false, true].into_iter().flat_map(|restore| {
        [false, true].into_iter().flat_map(move |reverse| {
            ["a.bin", "A.BIN", "b.bin"]
                .into_iter()
                .map(move |name| (restore, reverse, name))
        })
    }) {
        let mut job = job(&files);
        let mut damaged = b.clone();
        damaged[130] ^= 0x55;
        let (first, second) = if reverse {
            let second = job.member("b.bin", &a);
            (job.member(first_name, &damaged), second)
        } else {
            let first = job.member(first_name, &damaged);
            (first, job.member("b.bin", &a))
        };
        let mut object: Box<dyn Object> = if restore {
            crate::restore(&job.object.facts(), weaver_graph::Budget::default()).unwrap()
        } else {
            Box::new(job.object)
        };
        object
            .recovery_mut()
            .unwrap()
            .bind(&mut job.namespace, &mut Vec::new());
        assert!(
            object.recovery().unwrap().covers(second),
            "unique content must retain its binding"
        );
        let result = object
            .recovery_mut()
            .unwrap()
            .repair(&mut job.store, weaver_graph::Budget::default());
        assert!(
            matches!(result, RepairOutcome::Repaired { .. }),
            "{result:?}"
        );
        object
            .recovery_mut()
            .unwrap()
            .bind(&mut job.namespace, &mut Vec::new());
        assert!(
            object.recovery().unwrap().covers(first) && object.recovery().unwrap().covers(second)
        );
        assert_eq!(job.store.members[&first], b);
        assert_eq!(job.store.members[&second], a);
        assert_eq!(job.namespace.get(first).unwrap().name, "b.bin");
        assert_eq!(job.namespace.get(second).unwrap().name, "a.bin");
    }
}

#[test]
fn equal_content_claimants_remain_ambiguous_and_available_for_verification() {
    let a = payload(103, 259);
    let mut job = job(&par2_set("identity", &[("a.bin", &a)], 64, 1, 1));
    let first = job.member("first.bin", &a);
    let second = job.member("second.bin", &a);
    bind(&mut job);
    assert!(!job.object.covers(first) && !job.object.covers(second));
    let indexes: Vec<_> = job
        .object
        .sets
        .values()
        .filter(|set| set.usable().is_some())
        .collect();
    let found =
        super::super::bind::bind_sets(&indexes, &job.namespace, OWN, &mut Default::default());
    assert!(found.sets.iter().all(|set| set.is_empty()));
    assert_eq!(found.unnamed.len(), 2);
}

#[test]
fn cross_set_content_identity_outranks_an_earlier_filename_guess() {
    let a = payload(108, 259);
    let b = payload(109, 259);
    let mut files = par2_set("first", &[("a.bin", &a)], 64, 1, 1);
    files.extend(par2_set("second", &[("b.bin", &b)], 64, 1, 1));
    for reverse_members in [false, true] {
        let mut job = job(&files);
        let (a_member, b_member) = if reverse_members {
            let a_member = job.member("b.bin", &a);
            (a_member, job.member("a.bin", &b))
        } else {
            let b_member = job.member("a.bin", &b);
            (job.member("b.bin", &a), b_member)
        };
        let mut indexes: Vec<_> = job
            .object
            .sets
            .values()
            .filter(|set| set.usable().is_some())
            .collect();
        assert_eq!(indexes.len(), 2);
        for _ in 0..2 {
            let found = super::super::bind::bind_sets(
                &indexes,
                &job.namespace,
                OWN,
                &mut Default::default(),
            );
            assert!(found.unnamed.is_empty());
            for (index, bound) in indexes.iter().zip(found.sets) {
                let description = index.usable().unwrap().files.values().next().unwrap();
                let expected = if description.filename == "a.bin" {
                    a_member
                } else {
                    b_member
                };
                assert_eq!(bound.get(&description.file_id), Some(&expected));
            }
            indexes.reverse();
        }
    }
}
