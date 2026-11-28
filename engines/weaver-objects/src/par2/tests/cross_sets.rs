// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Binding over many sets: each member is matched only against the sets
// that describe a file like it, and a set is bound again only when what it
// binds may change. What it binds is what matching every member against
// every set, and binding every set, comes to.

use super::*;

// Everything binding decides, per set in order and per member.
pub(super) fn decided(job: &Job<Par2>, members: &[MemberId]) -> String {
    let mut out = String::new();
    for key in job.object.ordered() {
        let state = &job.object.sets[key];
        out.push_str(&format!(
            "{key:?} {:?} {:?} {:?}\n",
            state.members(),
            state.unnamed(),
            state.last
        ));
    }
    for member in members {
        // A posting removed has no record to say.
        let Some(record) = job.namespace.get(*member) else {
            out.push_str(&format!("{member:?} removed\n"));
            continue;
        };
        out.push_str(&format!(
            "{member:?} {} {:?} {} {:?}\n",
            record.name,
            record.length,
            job.object.covers(*member),
            job.object.lengths.get(member)
        ));
    }
    let mut rulings = Vec::new();
    job.object.sets(&mut rulings);
    out.push_str(&format!("{rulings:?}\n"));
    out
}

// Everything binding keeps beside what it decides: each set's verdict, the
// sets covering each member and those still to be said again, the unmatched
// members, the lengths, the sets binding and holding members, the members
// given up and whether a binding is due; and every record of the namespace.
// Not how often coverage changed: the scan gives every usable set an entry,
// so it binds them in another order and counts other moves.
pub(super) fn kept(job: &Job<Par2>) -> String {
    let object = &job.object;
    let mut out = String::new();
    for key in object.ordered() {
        out.push_str(&format!("{key:?} {:?}\n", object.sets[key].concluded()));
    }
    out.push_str(&format!(
        "{:?}\n{:?}\n{:?}\n{:?}\n{:?}\n{:?}\n{:?}\n{}\n",
        object.covered.by,
        object.covered.unfed,
        object.unmatched,
        object.lengths,
        object.binding,
        object.holding,
        object.given_up,
        object.bind_due(),
    ));
    for record in job.namespace.records() {
        out.push_str(&format!("{record:?}\n"));
    }
    out
}

// The members the object covers, and how often coverage changed.
pub(super) fn coverage(object: &Par2) -> (Vec<MemberId>, u64) {
    let mut members = Vec::new();
    object.covered_members(&mut members);
    (members, object.coverage_version())
}

// Two jobs alike, one binding by the catalogue and what changed and one by
// the scan and the whole: after every step they have decided and kept the
// same, in the same order, renamed the same members and cover the same; and
// each says coverage changed whenever what it covers did.
//
// Or, `whole`, the second binds by the catalogue too, its claims made stale
// before each binding, so each binding is a whole pass: then they also say
// coverage changed as often, the old binding's count beside the new one's.
pub(super) struct Twins {
    pub(super) jobs: [Job<Par2>; 2],
    pub(super) members: Vec<MemberId>,
    covered: [Option<(Vec<MemberId>, u64)>; 2],
    whole: bool,
}

impl Twins {
    pub(super) fn new(files: &[(String, Vec<u8>)]) -> Self {
        Self::of(files, false)
    }

    pub(super) fn of(files: &[(String, Vec<u8>)], whole: bool) -> Self {
        let mut jobs = [job(files), job(files)];
        jobs[1].object.scanning = !whole;
        Self {
            jobs,
            members: Vec::new(),
            covered: [None, None],
            whole,
        }
    }

    pub(super) fn each(&mut self, mut step: impl FnMut(&mut Job<Par2>) -> Option<MemberId>) {
        let found: Vec<_> = self.jobs.iter_mut().map(&mut step).collect();
        assert_eq!(found[0], found[1]);
        self.members.extend(found[0]);
    }

    pub(super) fn bind(&mut self, step: &str) {
        if self.whole {
            self.jobs[1].object.claims.stale();
        }
        let renamed: Vec<_> = self.jobs.iter_mut().map(bind).collect();
        assert_eq!(renamed[0], renamed[1], "{step}: renamed");
        let [catalogued, scanned] = &self.jobs;
        assert_eq!(
            decided(catalogued, &self.members),
            decided(scanned, &self.members),
            "{step}"
        );
        assert_eq!(kept(catalogued), kept(scanned), "{step}: kept");
        let now = [coverage(&catalogued.object), coverage(&scanned.object)];
        assert_eq!(now[0].0, now[1].0, "{step}: covered members");
        if self.whole {
            assert_eq!(now[0].1, now[1].1, "{step}: coverage version");
        }
        for (was, now) in self.covered.iter().zip(&now) {
            if let Some((members, version)) = was
                && *members != now.0
            {
                assert_ne!(*version, now.1, "{step}: coverage version");
            }
        }
        self.covered = now.map(Some);
        assert_eq!(
            catalogued.object.digests, scanned.object.digests,
            "{step}: fronts hashed"
        );
    }

    pub(super) fn repair(&mut self, step: &str) {
        let outcomes: Vec<_> = self
            .jobs
            .iter_mut()
            .map(|job| {
                job.object
                    .repair(&mut job.store, weaver_graph::Budget::default())
            })
            .collect();
        assert_eq!(outcomes[0], outcomes[1], "{step}");
        self.bind(step);
    }

    pub(super) fn restore(&mut self) {
        for (at, job) in self.jobs.iter_mut().enumerate() {
            let facts = job.object.facts();
            job.object = crate::par2::facts::par2(&facts).unwrap();
            job.object.scanning = at == 1 && !self.whole;
        }
        // A restored object is a new one: its versions start again.
        self.covered = [None, None];
    }
}

// Six sets whose names overlap and dispute: two describe `shared.bin`
// with different bytes, one describes it in capitals, two describe files
// whose names differ only in case, and one describes `a.bin`'s bytes under
// a name of its own. Members come under described names, swapped
// names, obfuscated names, with a damaged front, and in halves; the jobs
// bind, repair and restore between them.
#[test]
fn catalogued_binding_decides_what_the_scan_decides() {
    let a = payload(201, 3000);
    let shared_a = payload(202, 2500);
    let shared_b = payload(203, 2500);
    let shared_c = payload(204, 2600);
    let b = payload(205, 1800);
    let c = payload(206, 2200);
    let dup = payload(207, 1500);
    let dup_upper = payload(208, 1500);
    let unmatched = payload(209, 2200);
    let mut files = par2_set(
        "alpha",
        &[("shared.bin", &shared_a), ("a.bin", &a)],
        BLOCK,
        4,
        1,
    );
    files.extend(par2_set(
        "beta",
        &[("shared.bin", &shared_b), ("b.bin", &b)],
        BLOCK,
        4,
        1,
    ));
    files.extend(par2_set(
        "gamma",
        &[("SHARED.BIN", &shared_c), ("c.bin", &c)],
        BLOCK,
        8,
        1,
    ));
    files.extend(par2_set("delta", &[("dup.bin", &dup)], BLOCK, 2, 1));
    files.extend(par2_set("zeta", &[("Dup.Bin", &dup_upper)], BLOCK, 2, 1));
    files.extend(par2_set("epsilon", &[("e.bin", &a)], BLOCK, 2, 1));
    let mut twins = Twins::new(&files);
    assert_eq!(twins.jobs[0].object.sets.len(), 6);
    twins.bind("no members");

    // `shared.bin` posted with beta's bytes: content in beta outranks the
    // name alpha gives first. Alpha's bytes under gamma's capitals.
    twins.each(|job| Some(job.member("shared.bin", &shared_b)));
    twins.each(|job| Some(job.member("Shared.Bin", &shared_a)));
    twins.bind("shared names");

    // `a.bin`'s bytes answer in alpha and epsilon: alpha's. A name only
    // epsilon describes, with other bytes and no front yet.
    twins.each(|job| Some(job.member("posted-a.dat", &a)));
    twins.each(|job| {
        let member = job.namespace.announce("e.bin", Some(a.len() as u64));
        Some(member)
    });
    twins.bind("one file, two sets");

    // Zeta's bytes under delta's name, and a member in capitals that
    // answers to both names folded, with neither set's bytes.
    twins.each(|job| Some(job.member("dup.bin", &dup_upper)));
    twins.each(|job| Some(job.member("DUP.BIN", &payload(210, 1500))));
    twins.bind("case-variant names");

    // A member in halves, under an obfuscated name: unbound until its
    // front arrives, then bound by it.
    twins.each(|job| {
        let member = job.namespace.announce("x7f3.dat", Some(b.len() as u64));
        job.arrive(member, 0, &b[..900], Provenance::Fresh);
        job.store.members.insert(member, b.clone());
        Some(member)
    });
    twins.bind("a front in halves");
    let half = *twins.members.last().unwrap();
    twins.each(|job| {
        job.arrive(half, 900, &b[900..], Provenance::Fresh);
        None
    });
    twins.bind("the rest of it");

    // `c.bin` with its front damaged under an obfuscated name, and a
    // complete member of its length that is no file: both unnamed, both
    // possibly gamma's `c.bin`, until the repair reads them.
    twins.each(|job| Some(damaged_member(job, "q9.dat", &c, 100).0));
    twins.each(|job| Some(job.member("sample.dat", &unmatched)));
    twins.bind("unnamed members");
    let [shared, _, posted_a, _, dup_named, _, halves, damaged, _] = twins.members[..] else {
        panic!("members");
    };
    let object = &twins.jobs[0].object;
    for member in [shared, posted_a, dup_named, halves] {
        assert!(object.covers(member), "{member:?}");
    }
    assert!(!object.covers(damaged));
    assert!(
        object
            .sets
            .values()
            .any(|state| !state.unnamed().is_empty())
    );
    twins.repair("repaired");
    assert!(twins.jobs[0].object.covers(damaged));
    twins.bind("bound again");

    twins.restore();
    twins.bind("restored");
    twins.repair("repaired after restore");
    twins.bind("bound again after restore");
}

// One set describing what was posted and sets beside it describing files
// nothing was posted as: the first binding binds every set once; a binding
// with nothing new matches each member against the one set that describes
// it and binds that set alone, however many sets there are. The scan grows
// with every set, on every binding.
#[test]
fn binding_checks_grow_with_the_sets_that_describe_a_member() {
    let posted: Vec<(String, Vec<u8>)> = (0..3)
        .map(|at| (format!("part{at}.bin"), payload(300 + at, 1200)))
        .collect();
    let described: Vec<(&str, &[u8])> = posted
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut rows = Vec::new();
    for count in [2u64, 4, 8, 16] {
        let mut files = par2_set("real", &described, BLOCK, 1, 1);
        for other in 0..count - 1 {
            let bytes = payload(400 + other, 700);
            let name = format!("other{other}.bin");
            files.extend(par2_set(
                &format!("other{other}"),
                &[(&name, &bytes)],
                BLOCK,
                1,
                1,
            ));
        }
        let mut counted = Vec::new();
        for scanning in [false, true] {
            let mut job = job(&files);
            assert_eq!(job.object.sets.len() as u64, count);
            job.object.scanning = scanning;
            for (name, bytes) in &posted {
                job.member(name, bytes);
            }
            job.member("release.nfo", &payload(500, 333));
            let mut passes = Vec::new();
            for _ in 0..3 {
                let (checks, binds) = (job.object.set_checks, job.object.set_binds);
                bind(&mut job);
                passes.push((job.object.set_checks - checks, job.object.set_binds - binds));
            }
            let real = job.object.ordered()[0];
            assert_eq!(job.object.sets[&real].members().len(), 3);
            counted.push(passes);
        }
        rows.push((count, counted));
    }
    for (count, counted) in rows {
        let [catalogued, scanned] = &counted[..] else {
            unreachable!()
        };
        // The first binding matches each posted part against its set and
        // binds every set once: the new ones are all to be bound.
        assert_eq!(catalogued[0], (3, count as usize), "{count} sets");
        // After it, nothing. A bind with nothing new visits no posting, so
        // it matches no member against a set, and binds no set again.
        for pass in &catalogued[1..] {
            assert_eq!(*pass, (0, 0), "{count} sets");
        }
        // The scan matches the nfo against every set and binds every set,
        // on every binding.
        for pass in scanned {
            assert_eq!(pass.0, 3 + count as usize, "{count} sets: scan");
            assert_eq!(pass.1, count as usize, "{count} sets: scan");
        }
    }
}
