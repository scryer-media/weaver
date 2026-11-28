// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The passes over the holds ask only the held volumes something changed for
// since a pass settled them. Each step here runs on two jobs alike, one with
// those passes and one with the walk over every hold they replace, and both
// end every pass the same.

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::{Claim, Clean, PlacementMoves};

use super::*;

// What a pretend 7z says of itself, which a test changes: its placement
// number, whether it keeps one, and whether its map has come.
#[derive(Clone, Copy)]
struct Flipped {
    number: u64,
    numbered: bool,
    mapped: bool,
}

thread_local! {
    // What each pretend 7z becomes at its next delivery: as a real store,
    // it changes only when the tree hands it something, which marks it.
    static FLIPS: std::cell::RefCell<Vec<Flipped>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn flips_reset() {
    FLIPS.with_borrow_mut(Vec::clear);
}

fn flip_with(index: usize, change: impl FnOnce(&mut Flipped)) {
    FLIPS.with_borrow_mut(|flips| change(&mut flips[index]));
}

// Its map comes: the body has a place, and the number moves.
fn map(index: usize) {
    flip_with(index, |flip| {
        flip.mapped = true;
        flip.number += 1;
    });
}

// Its number moves, and the body still has no place.
fn nudge(index: usize) {
    flip_with(index, |flip| flip.number += 1);
}

// It keeps no number from now on: it is asked every pass.
fn numberless(index: usize) {
    flip_with(index, |flip| flip.numbered = false);
}

// It keeps a number again, the one it had, as an object lent out and
// taken back unmoved does.
fn number_back(index: usize) {
    flip_with(index, |flip| flip.numbered = true);
}

// It keeps a number again, one it never had.
fn renumber(index: usize) {
    flip_with(index, |flip| {
        flip.numbered = true;
        flip.number += 1;
    });
}

// A pretend 7z laid out as `Late` is, signature, body and map, whose map
// and placement number the test sets (`FLIPS`), taken up at its next
// delivery. Once mapped, its body is "flipN.bin".
struct Flip {
    index: usize,
    said: bool,
    live: Flipped,
}

impl Flip {
    fn new() -> Self {
        let made = Flipped {
            number: 1,
            numbered: true,
            mapped: false,
        };
        let index = FLIPS.with_borrow_mut(|flips| {
            flips.push(made);
            flips.len() - 1
        });
        Self {
            index,
            said: false,
            live: made,
        }
    }

    fn state(&self) -> Flipped {
        self.live
    }
}

impl Object for Flip {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::SevenZipStore
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        self.live = FLIPS.with_borrow(|flips| flips[self.index]);
        if !self.state().mapped {
            return;
        }
        if !self.said {
            self.said = true;
            out.push(Emit::Member {
                member: MemberId(0),
                name: format!("flip{}.bin", self.index),
                length: Some(body().len()),
            });
        }
        let range = ByteRange::at(delivery.offset, delivery.bytes.len() as u64);
        if let Some(part) = range.intersect(body()) {
            let start = (part.start - delivery.offset) as usize;
            out.push(Emit::Bytes {
                member: MemberId(0),
                offset: part.start - body().start,
                bytes: Bytes::copy_from_slice(&delivery.bytes[start..start + part.len() as usize])
                    .into(),
                provenance: delivery.provenance,
            });
        }
    }

    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}

    // A call that hands it nothing of its volumes: what was staged is taken
    // up, and nothing of its parent's members is marked.
    fn offer_password(&mut self, _: &str, _: &mut Vec<Emit>) {
        self.live = FLIPS.with_borrow(|flips| flips[self.index]);
    }
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn placement(&self) -> Option<u64> {
        let state = self.state();
        state.numbered.then_some(state.number)
    }

    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        if let Some(header) = range.intersect(ByteRange::new(0, body().start)) {
            out.push(Placement::Envelope {
                envelope: 0,
                range: header,
            });
        }
        if let Some(part) = range.intersect(body()) {
            out.push(if self.state().mapped {
                Placement::Member {
                    member: MemberId(0),
                    range: part.shift(-(body().start as i64)),
                }
            } else {
                Placement::Held { range: part }
            });
        }
        if let Some(tail) = range.intersect(ByteRange::new(body().end, u64::MAX)) {
            out.push(Placement::Envelope {
                envelope: 0,
                range: tail,
            });
        }
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
}

// The length of a pretend 7z: signature, body and map.
const FLIP_LEN: u64 = 40;

// Builds a `Flip` for each 7z, and the pretend RAR store holding one more
// 7z, "deep.7z", in a namespace of its own.
struct FlipBuild;

impl Builder for FlipBuild {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::RarStore => Some(Box::new(Store {
                announced: false,
                name: "deep.7z",
                length: FLIP_LEN,
                checksum_on_repair: None,
            })),
            Kind::SevenZipStore => Some(Box::new(Flip::new())),
            _ => None,
        }
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

// The 7z's bytes from `from` to `to`.
fn flip_bytes(from: u64, to: u64) -> Vec<u8> {
    let mut bytes = seven_z().to_vec();
    bytes.extend_from_slice(b"hello");
    bytes.extend_from_slice(MAP);
    bytes[from as usize..to as usize].to_vec()
}

struct Rig {
    job: Job,
}

impl Rig {
    fn new() -> Self {
        flips_reset();
        Self {
            job: Job::new(PLENTY),
        }
    }

    fn member(&mut self, name: &str, length: u64) -> MemberId {
        let member = self
            .job
            .tree
            .announce_root(name, Some(length), &mut Vec::new());
        self.job
            .writer
            .announced(
                &mut self.job.tree,
                &mut FlipBuild,
                &Source,
                NodeId::ROOT,
                member,
                &mut self.job.out,
            )
            .expect("announce");
        member
    }

    fn send(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        let arrival = Arrival {
            node: NodeId::ROOT,
            member,
            offset,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance: Provenance::Fresh,
        };
        self.job
            .writer
            .deliver(
                &mut self.job.tree,
                &mut FlipBuild,
                &Source,
                arrival,
                &mut self.job.out,
            )
            .expect("deliver");
    }

    fn close(&mut self, member: MemberId) {
        self.job
            .writer
            .close(
                &mut self.job.tree,
                &mut FlipBuild,
                &Source,
                NodeId::ROOT,
                member,
                &mut self.job.out,
            )
            .expect("close");
    }

    // The 7z's bytes from `from` to `to`, delivered.
    fn send_flip(&mut self, member: MemberId, from: u64, to: u64) {
        self.send(member, from, &flip_bytes(from, to));
    }

    // The holds paged out at a stop, and a writer rebuilt from their
    // records adopts them.
    fn restore(&mut self) {
        let job = &mut self.job;
        job.writer.page_out_holds().expect("page out");
        let nodes: Vec<NodeId> = job.writer.files.keys().copied().collect();
        let records: Vec<HoldRecord> = nodes
            .into_iter()
            .flat_map(|node| {
                job.writer
                    .hold_records(node)
                    .into_iter()
                    .map(move |(held, on_file)| HoldRecord {
                        node,
                        held,
                        on_file,
                    })
            })
            .collect();
        let mut writer = NamespaceWriter::new(
            1,
            job.dir.path().to_path_buf(),
            Arc::clone(&job.accountant),
            CapacityReader::unknown(),
        );
        writer.deliver_names();
        drop(std::mem::replace(&mut job.writer, writer));
        job.writer
            .restore_holds(&job.tree, &records)
            .expect("restore holds");
    }

    // The 7z under `member` takes up what was staged for it, through a
    // call that marks its node and nothing of the root's members.
    fn poke(&mut self, member: MemberId) {
        let (child, _) = self.job.tree.child_of(NodeId::ROOT, member).expect("a 7z");
        let mut effects = Vec::new();
        self.job
            .tree
            .offer_password(child, "", &mut FlipBuild, &mut effects);
        assert!(effects.is_empty(), "the offer says nothing");
    }

    fn visited(&self) -> usize {
        RELEASE_VISITED.get()
    }
}

// What one run of a scenario left: every pass, every effect said, and
// every file in the job's folder.
#[derive(Debug, PartialEq, Eq)]
struct Ended {
    passes: Vec<String>,
    said: String,
    files: Vec<(String, Vec<u8>)>,
}

fn ended(rig: Rig, passes: Vec<String>) -> Ended {
    let dir = rig.job.dir.path().to_path_buf();
    let said = format!("{:?}", rig.job.out).replace(&dir.display().to_string(), "<dir>");
    let mut files: Vec<(String, Vec<u8>)> = std::fs::read_dir(&dir)
        .expect("job folder")
        .map(|entry| {
            let entry = entry.expect("entry");
            let bytes = std::fs::read(entry.path()).unwrap_or_default();
            (entry.file_name().to_string_lossy().into_owned(), bytes)
        })
        .collect();
    files.sort();
    Ended {
        passes,
        said,
        files,
    }
}

// The scenario run with the passes over the holds as they are, then with
// the walk over every hold, each pass logged.
fn both(scenario: impl Fn(&mut Rig)) -> (Ended, Ended) {
    let run = |reference: bool| {
        RELEASE_REFERENCE.set(reference);
        RELEASE_LOG.set(Some(Vec::new()));
        let mut rig = Rig::new();
        scenario(&mut rig);
        let passes = RELEASE_LOG.take().unwrap_or_default();
        RELEASE_REFERENCE.set(false);
        ended(rig, passes)
    };
    (run(false), run(true))
}

// Several namespaces of holds, held volumes with an object and without one,
// members that get their object later, holds added to answered and to
// unanswered volumes, a placement number moving for one object of several,
// an object that keeps no number, volumes that lose their last hold, and a
// restore part way: every pass ends as the walk over every hold ends it.
#[test]
fn passes_over_the_unsettled_holds_end_every_pass_as_the_walk_over_every_hold() {
    let (kept, reference) = both(|rig| {
        let plain: Vec<MemberId> = (0..7)
            .map(|index| rig.member(&format!("p{index}.bin"), 4))
            .collect();
        let a = rig.member("a.7z", FLIP_LEN);
        let b = rig.member("b.7z", FLIP_LEN);
        let c = rig.member("c.7z", FLIP_LEN);
        let n = rig.member("n.7z", FLIP_LEN);
        let outer = rig.member("outer.rar", HEADER + FLIP_LEN);
        let late = rig.member("late.7z", FLIP_LEN);
        // A body before its signature: held with no object.
        rig.send_flip(late, 33, 35);
        // Each 7z's front and the body's first byte, held by its object.
        for member in [a, b, c, n] {
            rig.send_flip(member, 0, 33);
        }
        // The RAR store's 7z, in the store's namespace.
        let mut front = SIGNATURE.to_vec();
        front.extend_from_slice(&flip_bytes(0, 33));
        rig.send(outer, 0, &front);
        rig.send(plain[0], 0, b"data");
        // Holds added to answered volumes; one 7z stops keeping a number
        // as its hold comes.
        numberless(3);
        rig.send_flip(a, 33, 34);
        rig.send_flip(n, 33, 34);
        rig.send(outer, HEADER + 33, &flip_bytes(33, 34));
        // One object of several is mapped as a byte comes to it: its holds
        // go down.
        map(1);
        rig.send_flip(b, 33, 34);
        rig.send(plain[1], 0, b"data");
        // One moves and still places nothing.
        nudge(0);
        rig.send_flip(a, 34, 35);
        rig.send(plain[2], 0, b"data");
        // The late member gets its object.
        rig.send_flip(late, 0, 33);
        rig.send_flip(c, 33, 35);
        // The store's 7z is mapped.
        map(4);
        rig.send(outer, HEADER + 34, &flip_bytes(34, 35));
        rig.send(plain[3], 0, b"data");
        rig.restore();
        map(0);
        rig.send_flip(a, 35, 36);
        map(2);
        rig.send_flip(c, 35, 36);
        rig.send(plain[4], 0, b"data");
        // The numberless 7z keeps a number again, still unmapped, then is
        // mapped.
        renumber(3);
        rig.send_flip(n, 34, 35);
        rig.send(plain[5], 0, b"data");
        map(3);
        rig.send_flip(n, 35, 36);
        map(5);
        rig.send_flip(late, 35, 36);
        rig.send(plain[6], 0, b"data");
        rig.send_flip(b, 34, FLIP_LEN);
        for member in [a, c, n, late] {
            rig.send_flip(member, 36, FLIP_LEN);
        }
        rig.send(outer, HEADER + 35, &flip_bytes(35, FLIP_LEN));
    });
    assert!(kept.passes.len() > 20, "{} passes", kept.passes.len());
    assert_eq!(kept.passes.len(), reference.passes.len());
    for (at, (kept, reference)) in kept.passes.iter().zip(&reference.passes).enumerate() {
        assert_eq!(kept, reference, "pass {at}");
    }
    assert_eq!(kept.said, reference.said);
    assert_eq!(kept.files, reference.files);
    let flip = |index: usize| {
        kept.files
            .iter()
            .find(|(name, _)| *name == format!("flip{index}.bin"))
            .map(|(_, bytes)| bytes.clone())
    };
    for index in 0..6 {
        assert_eq!(flip(index).as_deref(), Some(&b"hello"[..]), "flip{index}");
    }
}

// An object keeps no number for a while, as one lent out does, and comes
// back under the number it had: its held volume is still asked when the
// number moves later, by a call that marks nothing of the root's members.
#[test]
fn a_number_that_comes_back_unmoved_still_asks_its_holds_when_it_moves() {
    let (kept, reference) = both(|rig| {
        let plain: Vec<MemberId> = (0..5)
            .map(|index| rig.member(&format!("p{index}.bin"), 4))
            .collect();
        let a = rig.member("a.7z", FLIP_LEN);
        rig.send_flip(a, 0, 33);
        rig.send(plain[0], 0, b"data");
        assert!(rig.job.writer.holds_asked(NodeId::ROOT, a));
        numberless(0);
        rig.poke(a);
        rig.send(plain[1], 0, b"data");
        number_back(0);
        rig.poke(a);
        rig.send(plain[2], 0, b"data");
        assert!(rig.job.writer.holds_asked(NodeId::ROOT, a));
        // The map comes, and moves the number.
        map(0);
        rig.poke(a);
        rig.send(plain[3], 0, b"data");
        assert_eq!(
            rig.job.writer.held_bytes(NodeId::ROOT),
            0,
            "the moved map releases the held byte"
        );
        rig.send_flip(a, 33, FLIP_LEN);
        rig.send(plain[4], 0, b"data");
    });
    assert_eq!(kept.passes, reference.passes);
    assert_eq!(kept.said, reference.said);
    assert_eq!(kept.files, reference.files);
}

// The same with the object lent out and taken back unmoved: while it is
// out it has no number, and back it has the one it had.
#[test]
fn an_object_taken_back_unmoved_still_asks_its_holds_when_it_moves() {
    let (kept, reference) = both(|rig| {
        let plain: Vec<MemberId> = (0..5)
            .map(|index| rig.member(&format!("p{index}.bin"), 4))
            .collect();
        let a = rig.member("a.7z", FLIP_LEN);
        rig.send_flip(a, 0, 33);
        rig.send(plain[0], 0, b"data");
        let (child, _) = rig.job.tree.child_of(NodeId::ROOT, a).expect("a 7z");
        let lent = rig.job.tree.lend_for_test(child).expect("lent");
        assert_eq!(rig.job.tree.placement(child), None, "no number while out");
        rig.send(plain[1], 0, b"data");
        let mut effects = Vec::new();
        rig.job
            .tree
            .take_back(child, lent, &mut FlipBuild, &mut effects);
        assert!(effects.is_empty());
        rig.send(plain[2], 0, b"data");
        assert!(rig.job.writer.holds_asked(NodeId::ROOT, a));
        // The member that waited behind the loan went to its file once the
        // object was back: the object's own byte is all that is held.
        assert_eq!(rig.job.writer.held_bytes(NodeId::ROOT), 1);
        map(0);
        rig.poke(a);
        rig.send(plain[3], 0, b"data");
        assert_eq!(
            rig.job.writer.held_bytes(NodeId::ROOT),
            0,
            "the moved map releases the held byte"
        );
        rig.send_flip(a, 33, FLIP_LEN);
        rig.send(plain[4], 0, b"data");
    });
    assert_eq!(kept.passes, reference.passes);
    assert_eq!(kept.said, reference.said);
    assert_eq!(kept.files, reference.files);
    for index in 0..5 {
        let name = format!("p{index}.bin");
        assert!(
            kept.files
                .iter()
                .any(|(file, bytes)| *file == name && bytes == b"data"),
            "{name}"
        );
    }
}

// A plain member's every byte arrives while a sibling's object is lent out:
// it waits behind the loan, undecided, its bytes held. Taken back, the
// member is decided a leaf, and when it closes with nothing more to come,
// its file holds its bytes and nothing of it stays held.
#[test]
fn a_member_decided_a_leaf_after_a_loan_writes_what_it_held() {
    let (kept, reference) = both(|rig| {
        let plain = rig.member("p.bin", 4);
        let a = rig.member("a.7z", FLIP_LEN);
        rig.send_flip(a, 0, 33);
        let (child, _) = rig.job.tree.child_of(NodeId::ROOT, a).expect("a 7z");
        let lent = rig.job.tree.lend_for_test(child).expect("lent");
        rig.send(plain, 0, b"data");
        assert!(
            rig.job.tree.is_undecided(NodeId::ROOT, plain),
            "behind the loan"
        );
        assert_eq!(rig.job.writer.held_bytes(NodeId::ROOT), 1 + 4);
        // Taken back as the job takes back an object from its work: what
        // it said goes through the writer, even nothing.
        let mut effects = Vec::new();
        rig.job
            .tree
            .take_back(child, lent, &mut FlipBuild, &mut effects);
        assert!(effects.is_empty());
        let job = &mut rig.job;
        job.writer
            .apply_all(
                &mut job.tree,
                &mut FlipBuild,
                &Source,
                effects,
                &mut job.out,
            )
            .expect("taken back");
        assert!(!rig.job.tree.is_undecided(NodeId::ROOT, plain));
        assert_eq!(rig.job.tree.child_of(NodeId::ROOT, plain), None, "a leaf");
        rig.close(plain);
        assert_eq!(
            rig.job.writer.held_bytes(NodeId::ROOT),
            1,
            "only the 7z's own byte is held"
        );
        map(0);
        rig.send_flip(a, 33, FLIP_LEN);
        assert_eq!(rig.job.writer.held_bytes(NodeId::ROOT), 0);
    });
    assert_eq!(kept.passes, reference.passes);
    assert_eq!(kept.said, reference.said);
    assert_eq!(kept.files, reference.files);
    let file = |name: &str| {
        kept.files
            .iter()
            .find(|(file, _)| file == name)
            .map(|(_, bytes)| bytes.clone())
    };
    assert_eq!(file("p.bin").as_deref(), Some(&b"data"[..]));
    assert_eq!(file("flip0.bin").as_deref(), Some(&b"hello"[..]));
}

// A plain member's every byte arrives, and it closes, while a sibling's
// object is lent out. Undecided behind the loan, its close settles it: what
// it held goes to its own file before the file is closed, so the close finds
// a file to close. Taken back, the member is decided a leaf, and its file
// stays closed and holds its bytes; nothing of it is held.
#[test]
fn a_member_closed_behind_a_loan_has_its_file_closed_when_its_bytes_go_down() {
    let (kept, reference) = both(|rig| {
        let plain = rig.member("p.bin", 4);
        let a = rig.member("a.7z", FLIP_LEN);
        rig.send_flip(a, 0, 33);
        let (child, _) = rig.job.tree.child_of(NodeId::ROOT, a).expect("a 7z");
        let lent = rig.job.tree.lend_for_test(child).expect("lent");
        rig.send(plain, 0, b"data");
        assert_eq!(rig.job.writer.held_bytes(NodeId::ROOT), 1 + 4);
        rig.close(plain);
        assert!(
            rig.job.tree.is_undecided(NodeId::ROOT, plain),
            "behind the loan"
        );
        assert_eq!(
            rig.job.writer.held_bytes(NodeId::ROOT),
            1,
            "only the 7z's own byte is held"
        );
        // The member has a file, and it is not open.
        let closed = |rig: &Rig| {
            let file = |path: &std::path::Path| path.ends_with("p.bin");
            rig.job
                .writer
                .files(NodeId::ROOT)
                .any(|(_, path)| file(path))
                && !rig.job.writer.open_files().iter().any(|path| file(path))
        };
        assert!(closed(rig), "closed behind the loan");
        let mut effects = Vec::new();
        rig.job
            .tree
            .take_back(child, lent, &mut FlipBuild, &mut effects);
        let job = &mut rig.job;
        job.writer
            .apply_all(
                &mut job.tree,
                &mut FlipBuild,
                &Source,
                effects,
                &mut job.out,
            )
            .expect("taken back");
        assert_eq!(rig.job.tree.child_of(NodeId::ROOT, plain), None, "a leaf");
        assert!(!rig.job.tree.is_undecided(NodeId::ROOT, plain));
        assert_eq!(rig.job.writer.held_bytes(NodeId::ROOT), 1);
        assert!(closed(rig), "closed once taken back");
        map(0);
        rig.send_flip(a, 33, FLIP_LEN);
        assert_eq!(rig.job.writer.held_bytes(NodeId::ROOT), 0);
    });
    assert_eq!(kept.passes, reference.passes);
    assert_eq!(kept.said, reference.said);
    assert_eq!(kept.files, reference.files);
    let file = |name: &str| {
        kept.files
            .iter()
            .find(|(file, _)| file == name)
            .map(|(_, bytes)| bytes.clone())
    };
    assert_eq!(file("p.bin").as_deref(), Some(&b"data"[..]));
    assert_eq!(file("flip0.bin").as_deref(), Some(&b"hello"[..]));
}

// V held volumes none of whose objects move, then D deliveries each adding
// a hold to one of them: each delivery's passes visit the one volume it
// marked, so the passes visit D + V held volumes, not V for every pass.
#[test]
fn passes_visit_each_held_volume_once_per_change_not_once_per_pass() {
    for volumes in [200, 400] {
        let mut rig = Rig::new();
        let stores: Vec<MemberId> = (0..volumes)
            .map(|index| rig.member(&format!("s{index}.7z"), FLIP_LEN))
            .collect();
        RELEASE_VISITED.set(0);
        for store in &stores {
            rig.send_flip(*store, 0, 33);
        }
        let deliveries = volumes;
        for store in &stores {
            rig.send_flip(*store, 33, 34);
        }
        assert_eq!(
            rig.visited(),
            deliveries + volumes,
            "{volumes} volumes, {deliveries} deliveries"
        );
        let held = rig.job.writer.held_bytes(NodeId::ROOT);
        assert_eq!(held, 2 * volumes as u64, "{volumes}: every hold stays");
    }
}

// A member held without an object through many passes is asked by the
// pass after it gets one; until then no pass visits it again.
#[test]
fn a_member_that_gets_its_object_late_is_asked_by_the_next_pass() {
    let mut rig = Rig::new();
    let plain: Vec<MemberId> = (0..50)
        .map(|index| rig.member(&format!("p{index}.bin"), 4))
        .collect();
    let late = rig.member("late.7z", FLIP_LEN);
    rig.send_flip(late, 33, 35);
    assert!(!rig.job.writer.holds_asked(NodeId::ROOT, late));
    RELEASE_VISITED.set(0);
    for member in &plain {
        rig.send(*member, 0, b"data");
    }
    assert_eq!(rig.visited(), 0, "no pass visits it while nothing changed");
    rig.send_flip(late, 0, 33);
    assert!(rig.job.writer.holds_asked(NodeId::ROOT, late), "asked");
    assert_eq!(rig.visited(), 1);
    map(0);
    rig.send_flip(late, 35, 36);
    assert_eq!(rig.job.writer.held_bytes(NodeId::ROOT), 0);
    rig.send_flip(late, 36, FLIP_LEN);
    assert_eq!(rig.job.file("flip0.bin"), b"hello");
}

// The placement number of one object of two moves as a byte comes to it:
// the next pass visits that object's held volume once and locates each of
// its holds once, and the pass after visits nothing.
#[test]
fn a_moved_placement_asks_every_hold_of_its_object_once() {
    let mut rig = Rig::new();
    let plain = rig.member("p.bin", 12);
    let a = rig.member("a.7z", FLIP_LEN);
    let b = rig.member("b.7z", FLIP_LEN);
    for member in [a, b] {
        rig.send_flip(member, 0, 33);
        for at in 33..36 {
            rig.send_flip(member, at, at + 1);
        }
    }
    let holds = |rig: &Rig, member: MemberId| {
        rig.job.writer.files[&NodeId::ROOT]
            .holds
            .held_ranges(VolumeIndex(member.0))
            .len()
    };
    assert_eq!(holds(&rig, a), 4);
    assert_eq!(holds(&rig, b), 4);
    nudge(0);
    RELEASE_VISITED.set(0);
    RELEASE_LOCATED.set(0);
    rig.send_flip(a, 36, 37);
    assert_eq!(rig.visited(), 1, "the moved object's volume");
    assert_eq!(
        RELEASE_LOCATED.get(),
        5,
        "each of its holds once, the new one with them"
    );
    rig.send(plain, 0, b"data");
    assert_eq!(rig.visited(), 1, "nothing moved since");
    assert_eq!(RELEASE_LOCATED.get(), 5);
}

// What a pretend shelf of volumes does at its next delivery: maps one
// volume, or every one.
#[derive(Clone, Copy)]
enum Staged {
    Map(VolumeIndex),
    MapAll,
}

thread_local! {
    static SHELF_STAGED: std::cell::RefCell<Vec<Staged>> = const { std::cell::RefCell::new(Vec::new()) };
}

// A pretend shelf volume: its header, its body, and a trailer.
const SHELF_HEAD: u64 = 64;
const SHELF_BODY: u64 = SHELF_HEAD + 8;
const SHELF_LEN: u64 = SHELF_BODY + 64;

fn shelf_name(volume: u32) -> String {
    format!("shelf{volume:03}.rar")
}

fn shelf_volume(name: &str) -> Option<VolumeIndex> {
    name.strip_prefix("shelf")?
        .strip_suffix(".rar")?
        .parse()
        .ok()
        .map(VolumeIndex)
}

// A volume's bytes: the signature, header filler, a body of its number,
// and trailer filler.
fn shelf_bytes(volume: u32, from: u64, to: u64) -> Vec<u8> {
    let mut bytes = SIGNATURE.to_vec();
    bytes.resize(SHELF_HEAD as usize, 0x5a);
    bytes.resize(SHELF_BODY as usize, b'0' + volume as u8);
    bytes.resize(SHELF_LEN as usize, 0xa5);
    bytes[from as usize..to as usize].to_vec()
}

// A pretend store of many volumes, each a header and trailer kept as its
// envelope and a body held until the volume is mapped, then the body of a
// member named for the volume, numbered as the store says its members, in
// turn: a map says the member of every volume it maps that the store has
// had a delivery to, in the volumes' order. Every delivery moves its placement number for the volume it came to
// alone; a staged map moves it for that volume, or for every one.
#[derive(Default)]
struct Shelf {
    number: u64,
    // The number taken at its last move of every volume, and each volume
    // moved alone since, under its latest number.
    all_at: u64,
    moved: BTreeMap<VolumeIndex, u64>,
    mapped: BTreeSet<VolumeIndex>,
    all_mapped: bool,
    seen: BTreeSet<VolumeIndex>,
    said: BTreeMap<VolumeIndex, MemberId>,
}

impl Shelf {
    fn is_mapped(&self, volume: VolumeIndex) -> bool {
        self.all_mapped || self.mapped.contains(&volume)
    }
}

impl Object for Shelf {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        let volume = delivery.volume;
        self.seen.insert(volume);
        self.number += 1;
        self.moved.insert(volume, self.number);
        for staged in SHELF_STAGED.with_borrow_mut(std::mem::take) {
            self.number += 1;
            match staged {
                Staged::Map(mapped) => {
                    self.mapped.insert(mapped);
                    self.moved.insert(mapped, self.number);
                }
                Staged::MapAll => {
                    self.all_mapped = true;
                    self.all_at = self.number;
                    self.moved.clear();
                }
            }
        }
        let unsaid: Vec<VolumeIndex> = self
            .seen
            .iter()
            .filter(|seen| self.is_mapped(**seen) && !self.said.contains_key(seen))
            .copied()
            .collect();
        for unsaid in unsaid {
            let member = MemberId(self.said.len() as u32);
            self.said.insert(unsaid, member);
            out.push(Emit::Member {
                member,
                name: format!("shelf{}.bin", unsaid.0),
                length: Some(SHELF_BODY - SHELF_HEAD),
            });
        }
        let Some(&member) = self.said.get(&volume) else {
            return;
        };
        let range = ByteRange::at(delivery.offset, delivery.bytes.len() as u64);
        if let Some(part) = range.intersect(ByteRange::new(SHELF_HEAD, SHELF_BODY)) {
            let start = (part.start - delivery.offset) as usize;
            out.push(Emit::Bytes {
                member,
                offset: part.start - SHELF_HEAD,
                bytes: Bytes::copy_from_slice(&delivery.bytes[start..start + part.len() as usize])
                    .into(),
                provenance: delivery.provenance,
            });
        }
    }

    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn placement(&self) -> Option<u64> {
        Some(self.number)
    }

    fn placement_moves(&self, since: u64) -> PlacementMoves {
        if since < self.all_at || since > self.number {
            return PlacementMoves::All;
        }
        PlacementMoves::Volumes(
            self.moved
                .iter()
                .filter(|(_, at)| **at > since)
                .map(|(volume, _)| *volume)
                .collect(),
        )
    }

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        if let Some(header) = range.intersect(ByteRange::new(0, SHELF_HEAD)) {
            out.push(Placement::Envelope {
                envelope: volume.0,
                range: header,
            });
        }
        if let Some(part) = range.intersect(ByteRange::new(SHELF_HEAD, SHELF_BODY)) {
            out.push(match self.said.get(&volume) {
                Some(&member) if self.is_mapped(volume) => Placement::Member {
                    member,
                    range: part.shift(-(SHELF_HEAD as i64)),
                },
                _ => Placement::Held { range: part },
            });
        }
        if let Some(trailer) = range.intersect(ByteRange::new(SHELF_BODY, SHELF_LEN)) {
            out.push(Placement::Envelope {
                envelope: volume.0,
                range: trailer,
            });
        }
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }

    fn claim(&mut self, name: &str, _: Clean<'_>, _: Option<u64>) -> Claim {
        shelf_volume(name)
            .map(|volume| vec![volume].into())
            .unwrap_or_default()
    }
}

struct ShelfBuild;

impl Builder for ShelfBuild {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| Box::new(Shelf::default()) as Box<dyn Object>)
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

// A job of one shelf, its volumes and plain members beside it.
struct ShelfRig {
    job: Job,
    volumes: Vec<MemberId>,
}

impl ShelfRig {
    fn new(volumes: u32) -> Self {
        SHELF_STAGED.with_borrow_mut(Vec::clear);
        let mut rig = Self {
            job: Job::new(PLENTY),
            volumes: Vec::new(),
        };
        rig.volumes = (0..volumes)
            .map(|volume| rig.member(&shelf_name(volume), SHELF_LEN))
            .collect();
        rig
    }

    fn member(&mut self, name: &str, length: u64) -> MemberId {
        let member = self
            .job
            .tree
            .announce_root(name, Some(length), &mut Vec::new());
        self.job
            .writer
            .announced(
                &mut self.job.tree,
                &mut ShelfBuild,
                &Source,
                NodeId::ROOT,
                member,
                &mut self.job.out,
            )
            .expect("announce");
        member
    }

    fn send(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        let arrival = Arrival {
            node: NodeId::ROOT,
            member,
            offset,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance: Provenance::Fresh,
        };
        self.job
            .writer
            .deliver(
                &mut self.job.tree,
                &mut ShelfBuild,
                &Source,
                arrival,
                &mut self.job.out,
            )
            .expect("deliver");
    }

    // Volume `volume`'s bytes from `from` to `to`, delivered.
    fn send_volume(&mut self, volume: u32, from: u64, to: u64) {
        let member = self.volumes[volume as usize];
        self.send(member, from, &shelf_bytes(volume, from, to));
    }

    // Every volume's header and the first of its body, held: one store
    // holds them all.
    fn hold_every_volume(&mut self) {
        for volume in 0..self.volumes.len() as u32 {
            self.send_volume(volume, 0, SHELF_HEAD);
            self.send_volume(volume, SHELF_HEAD, SHELF_HEAD + 2);
        }
        let shelf = self.job.tree.child_of(NodeId::ROOT, self.volumes[0]);
        for (at, member) in self.volumes.iter().enumerate() {
            let (child, volume) = self
                .job
                .tree
                .child_of(NodeId::ROOT, *member)
                .unwrap_or_else(|| panic!("shelf volume {at} unbound"));
            assert_eq!(Some(child), shelf.map(|(child, _)| child), "one shelf");
            assert_eq!(volume, VolumeIndex(at as u32));
        }
        assert_eq!(
            self.job.writer.held_bytes(NodeId::ROOT),
            2 * self.volumes.len() as u64
        );
    }

    fn visited(&self) -> usize {
        RELEASE_VISITED.get()
    }
}

// Run with the passes over the holds as they are, then with the walk over
// every hold, each pass logged.
fn both_shelves(scenario: impl Fn(&mut ShelfRig)) -> (Ended, Ended) {
    let run = |reference: bool| {
        RELEASE_REFERENCE.set(reference);
        RELEASE_LOG.set(Some(Vec::new()));
        let mut rig = ShelfRig::new(6);
        scenario(&mut rig);
        let passes = RELEASE_LOG.take().unwrap_or_default();
        RELEASE_REFERENCE.set(false);
        ended(Rig { job: rig.job }, passes)
    };
    (run(false), run(true))
}

// One store holds bytes of every volume while one of its volumes takes
// delivery after delivery, a hold added to it, then one volume is mapped
// alone and later every one: every pass ends as the walk over every hold
// ends it, and each volume's member is written whole.
#[test]
fn a_store_whose_volumes_move_one_at_a_time_ends_every_pass_as_the_walk_over_every_hold() {
    let (kept, reference) = both_shelves(|rig| {
        let plain: Vec<MemberId> = (0..4)
            .map(|index| rig.member(&format!("p{index}.bin"), 4))
            .collect();
        rig.hold_every_volume();
        rig.send(plain[0], 0, b"data");
        for at in SHELF_BODY..SHELF_BODY + 8 {
            rig.send_volume(2, at, at + 1);
        }
        rig.send_volume(2, SHELF_HEAD + 2, SHELF_HEAD + 3);
        rig.send(plain[1], 0, b"data");
        // One volume is mapped alone: its holds go down, and no other's.
        SHELF_STAGED.with_borrow_mut(|staged| staged.push(Staged::Map(VolumeIndex(4))));
        rig.send_volume(4, SHELF_BODY, SHELF_BODY + 1);
        rig.send(plain[2], 0, b"data");
        // Every volume is mapped.
        SHELF_STAGED.with_borrow_mut(|staged| staged.push(Staged::MapAll));
        rig.send_volume(0, SHELF_BODY, SHELF_BODY + 1);
        rig.send(plain[3], 0, b"data");
        assert_eq!(
            rig.job.writer.held_bytes(NodeId::ROOT),
            0,
            "every hold went down"
        );
        for volume in 0..6 {
            let from = match volume {
                2 => SHELF_HEAD + 3,
                _ => SHELF_HEAD + 2,
            };
            rig.send_volume(volume, from, SHELF_BODY);
        }
    });
    assert!(kept.passes.len() > 20, "{} passes", kept.passes.len());
    assert_eq!(kept.passes.len(), reference.passes.len());
    for (at, (kept, reference)) in kept.passes.iter().zip(&reference.passes).enumerate() {
        assert_eq!(kept, reference, "pass {at}");
    }
    assert_eq!(kept.said, reference.said);
    assert_eq!(kept.files, reference.files);
    // Each volume's member, under its name.
    let shelves: Vec<_> = kept
        .files
        .iter()
        .filter(|(file, _)| file.starts_with("shelf"))
        .collect();
    let expected: Vec<(String, Vec<u8>)> = (0..6u8)
        .map(|volume| {
            (
                format!("shelf{volume}.bin"),
                vec![b'0' + volume; (SHELF_BODY - SHELF_HEAD) as usize],
            )
        })
        .collect();
    assert_eq!(shelves, expected.iter().collect::<Vec<_>>());
}

// V held volumes of one store, then D deliveries to one of them, each
// moving the store's number for that volume alone: each delivery's passes
// visit that one held volume, so the passes visit V + D held volumes, not
// V for every delivery.
#[test]
fn deliveries_to_one_volume_of_a_store_visit_that_volume_alone() {
    for volumes in [50, 100] {
        let mut rig = ShelfRig::new(volumes);
        RELEASE_VISITED.set(0);
        rig.hold_every_volume();
        assert_eq!(rig.visited(), volumes as usize, "each volume's first hold");
        let deliveries = 48;
        for at in SHELF_BODY..SHELF_BODY + deliveries {
            rig.send_volume(0, at, at + 1);
        }
        assert_eq!(
            rig.visited(),
            volumes as usize + deliveries as usize,
            "{volumes} volumes, {deliveries} deliveries"
        );
        assert_eq!(
            rig.job.writer.held_bytes(NodeId::ROOT),
            2 * u64::from(volumes),
            "every hold stays"
        );
    }
}
