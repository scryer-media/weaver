// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Work loans held out while the job hears what a server sends: every loan
// stays in the test's hand until the test gives it back, so articles, a
// send-back, a loss or a password reach the job while the object is out,
// as they do on the pool when the work takes longer than the next article.

use std::path::Path;
use std::sync::Arc;

use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{NodeId, Object};

use super::GraphJob;
use crate::operations::disk::CapacityReader;
use crate::pipeline::graph::engine::{RunMail, Runs};
use crate::pipeline::graph::hardware::Hardware;
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::root::tests::{decoded, file};
use crate::pipeline::graph::status::RepairSlots;
use crate::pipeline::tests::engines::loans::{FRAME, Kind, SECRET, body, stored_zip, volumes};

type Loan = (NodeId, u64, Box<dyn Object>);

// The job runs under one slot: under none it lends nothing.
const LIMIT: usize = 1;

// How the loans go: kept in hand, or done on the loop as they are while a
// checkpoint is owed.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Held,
    Inline,
}

#[derive(Clone, Copy)]
enum Serve {
    InOrder,
    Backwards,
    // Each volume's last article first, as a seek for an archive's
    // directory reads it, then the rest in order.
    TailFirst,
}

struct Rig {
    job: GraphJob,
    slots: RepairSlots,
    // Each root volume's bytes.
    volumes: Vec<Vec<u8>>,
    in_hand: Vec<Loan>,
    // Whether any loan was ever in hand.
    lent: bool,
    directory: tempfile::TempDir,
}

impl Rig {
    fn new(posts: Vec<(String, Vec<u8>)>, candidates: &[&str], mode: Mode) -> Self {
        let files: Vec<_> = posts
            .iter()
            .map(|(name, bytes)| {
                let sizes: Vec<u32> = bytes
                    .chunks(FRAME)
                    .map(|chunk| chunk.len() as u32)
                    .collect();
                file(name, &sizes)
            })
            .collect();
        let directory = tempfile::tempdir().unwrap();
        let accountant = Arc::new(HoldsAccountant::new(HoldBudgets {
            resident_limit: u64::MAX,
            scratch_total: u64::MAX,
            scratch_ceiling: u64::MAX,
            disk_reserve: 0,
        }));
        let writer = NamespaceWriter::new(
            1,
            directory.path().to_owned(),
            Arc::clone(&accountant),
            CapacityReader::unknown(),
        );
        let runs = Runs::new(1, 0, RunMail::default().sender(), accountant);
        let mut job = GraphJob::new(
            &files,
            Box::new(Hardware::default()),
            writer,
            runs,
            u32::MAX,
        )
        .offering(candidates.iter().map(|secret| secret.to_string()).collect());
        match mode {
            Mode::Held => job.hold_loans(),
            Mode::Inline => job.owe_checkpoint(),
        }
        Self {
            job,
            slots: RepairSlots::default(),
            volumes: posts.into_iter().map(|(_, bytes)| bytes).collect(),
            in_hand: Vec::new(),
            lent: false,
            directory,
        }
    }

    // Every article of every volume, volume by volume, in the order served.
    fn articles(&self, serve: Serve) -> Vec<(u32, u32)> {
        let mut articles: Vec<(u32, u32)> = self
            .volumes
            .iter()
            .enumerate()
            .flat_map(|(volume, bytes)| {
                (0..bytes.len().div_ceil(FRAME)).map(move |ordinal| (volume as u32, ordinal as u32))
            })
            .collect();
        match serve {
            Serve::InOrder => {}
            Serve::Backwards => articles.reverse(),
            Serve::TailFirst => {
                let mut tails = Vec::new();
                for volume in 0..self.volumes.len() as u32 {
                    let last = articles.iter().rposition(|(of, _)| *of == volume).unwrap();
                    tails.push(articles.remove(last));
                }
                tails.extend(articles);
                articles = tails;
            }
        }
        articles
    }

    fn bytes(&self, (volume, ordinal): (u32, u32)) -> (u64, Vec<u8>, u64) {
        let all = &self.volumes[volume as usize];
        let start = ordinal as usize * FRAME;
        let end = (start + FRAME).min(all.len());
        (start as u64, all[start..end].to_vec(), all.len() as u64)
    }

    fn take(&mut self) {
        let loans = self.job.held_loans();
        self.lent |= !loans.is_empty();
        self.in_hand.extend(loans);
    }

    fn intake(&mut self, article: (u32, u32)) {
        let (offset, bytes, length) = self.bytes(article);
        self.serve(article, offset, &bytes, length);
    }

    // A copy whose bytes are wrong, though the decoder took it whole: its
    // neighbours refute it later and it is sent back.
    fn intake_damaged(&mut self, article: (u32, u32)) {
        let (offset, mut bytes, length) = self.bytes(article);
        for byte in &mut bytes {
            *byte ^= 0x5a;
        }
        self.serve(article, offset, &bytes, length);
    }

    fn serve(&mut self, (volume, ordinal): (u32, u32), offset: u64, bytes: &[u8], length: u64) {
        self.job
            .intake(
                decoded(volume, ordinal, offset, bytes, Some(length)),
                &mut self.slots,
                LIMIT,
            )
            .unwrap();
        self.take();
    }

    fn send_back(&mut self, (volume, ordinal): (u32, u32)) {
        self.job
            .send_back(weaver_graph::VolumeIndex(volume), ordinal, LIMIT)
            .unwrap();
        self.take();
    }

    fn lost(&mut self, (volume, ordinal): (u32, u32)) {
        self.job
            .lost(
                weaver_graph::VolumeIndex(volume),
                ordinal,
                &mut self.slots,
                LIMIT,
            )
            .unwrap();
        self.take();
    }

    // Every loan in hand comes back, in the order lent; what their return
    // lends is kept in hand.
    fn give_back(&mut self) {
        for loan in std::mem::take(&mut self.in_hand) {
            self.job.return_loan(loan, &mut self.slots, LIMIT).unwrap();
            self.take();
        }
    }

    // Every loan comes back, and every one their return lends, until none
    // is out; then the gate lends what is due, as it does before it rules.
    fn settle(&mut self) {
        loop {
            self.job.lend_work(LIMIT).unwrap();
            self.take();
            if self.in_hand.is_empty() {
                break;
            }
            self.give_back();
        }
        assert!(self.job.tree.working().is_empty(), "work left due");
        assert!(!self.job.work_pending(), "work left pending");
    }

    // Whether a file of the job's holds the member whole.
    fn delivered(&self) -> bool {
        holds(self.directory.path(), &body())
    }
}

pub(in crate::pipeline::graph::job) fn holds(path: &Path, want: &[u8]) -> bool {
    std::fs::read_dir(path).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        if path.is_dir() {
            holds(&path, want)
        } else {
            std::fs::read(&path).is_ok_and(|bytes| bytes == want)
        }
    })
}

#[derive(Clone, Copy)]
enum Shape {
    Flat,
    // Inside a stored outer zip.
    Nested,
}

fn posts(kind: Kind, shape: Shape) -> Vec<(String, Vec<u8>)> {
    let volumes = volumes(kind, &body(), 2);
    match shape {
        Shape::Flat => volumes,
        Shape::Nested => {
            assert_eq!(volumes.len(), 1, "a nested post wraps one archive");
            vec![("outer.zip".into(), stored_zip("lantern.zip", &volumes[0].1))]
        }
    }
}

// Every article served in turn, each loan given back after the article
// that lent it.
fn served(kind: Kind, shape: Shape, serve: Serve, candidates: &[&str]) {
    let mut rig = Rig::new(posts(kind, shape), candidates, Mode::Held);
    for article in rig.articles(serve) {
        rig.intake(article);
        rig.give_back();
    }
    rig.settle();
    assert!(rig.lent, "a loan went out");
    assert!(rig.delivered(), "the member delivered whole");
    assert_eq!(rig.job.phase(), RootPhase::Done);
}

// Every article served while the first loan is out, none given back until
// the last is in: the job is not done while a loan is out, nor while work
// is due that nothing lent.
fn held_to_the_end(kind: Kind, shape: Shape, serve: Serve) {
    let mut rig = Rig::new(posts(kind, shape), &[SECRET], Mode::Held);
    for article in rig.articles(serve) {
        rig.intake(article);
    }
    assert!(!rig.in_hand.is_empty(), "a loan is out at the last article");
    assert!(rig.job.work_pending(), "a loan out is pending work");
    assert_ne!(rig.job.phase(), RootPhase::Done, "not done with a loan out");
    assert!(!rig.delivered(), "nothing delivered with a loan out");
    rig.settle();
    assert!(rig.delivered(), "the member delivered whole");
}

// Every article served with no slot to run in, as a paused job is: no loan
// goes out, though work is due. Once a slot opens, the gate lends it.
#[test]
fn nothing_is_lent_without_a_slot_and_the_due_work_goes_out_once_one_opens() {
    let mut rig = Rig::new(posts(Kind::ZipCryptoSized, Shape::Flat), &[SECRET], Mode::Held);
    for (volume, ordinal) in rig.articles(Serve::InOrder) {
        let (offset, bytes, length) = rig.bytes((volume, ordinal));
        rig.job
            .intake(
                decoded(volume, ordinal, offset, &bytes, Some(length)),
                &mut rig.slots,
                0,
            )
            .unwrap();
        rig.take();
    }
    assert!(!rig.lent, "a loan went out with no slot");
    assert!(rig.job.work_pending(), "work is due");
    rig.job.lend_work(0).unwrap();
    rig.take();
    assert!(!rig.lent, "a loan went out with no slot");
    rig.job.lend_work(LIMIT).unwrap();
    rig.take();
    assert!(rig.lent, "the due work went out once a slot opened");
    rig.settle();
    assert!(rig.delivered(), "the member delivered whole");
}

// Where in the served order the bad copy comes.
#[derive(Clone, Copy)]
enum At {
    First,
    Middle,
    Late,
}

// The article at `at` first comes wrong. Once `lag` more articles are in
// and, held, a loan is out, its neighbours refute it and it is sent back,
// its good copy comes at once, and the loans then come back. A post whose
// object lends nothing before its last article is sent back after it.
fn sent_back(kind: Kind, shape: Shape, serve: Serve, mode: Mode, at: At, lag: usize) {
    let mut rig = Rig::new(posts(kind, shape), &[SECRET], mode);
    let articles = rig.articles(serve);
    let n = articles.len();
    let bad = match at {
        // Early, past what opens the archive and what holds its directory,
        // in any order served.
        At::First => 2,
        At::Middle => n / 2,
        At::Late => n - 1 - lag,
    };
    let mut done = false;
    for (index, article) in articles.iter().copied().enumerate() {
        if index == bad {
            rig.intake_damaged(article);
        } else {
            rig.intake(article);
        }
        if !done && index >= bad + lag && (mode == Mode::Inline || !rig.in_hand.is_empty()) {
            rig.send_back(articles[bad]);
            rig.intake(articles[bad]);
            rig.give_back();
            done = true;
        } else if done || mode == Mode::Inline {
            rig.give_back();
        }
    }
    if !done {
        assert!(!rig.in_hand.is_empty(), "a loan is out at the send-back");
        rig.send_back(articles[bad]);
        rig.intake(articles[bad]);
    }
    rig.settle();
    assert!(rig.delivered(), "the member delivered whole");
    assert_eq!(rig.job.phase(), RootPhase::Done);
}

// The bad copy is sent back once two more articles are in and a loan is
// out; a post whose object lends nothing before its last article is sent
// back after it, with the loan out. `then` is what follows the send-back.
fn send_back_with_a_loan_out(kind: Kind, shape: Shape, serve: Serve, then: fn(&mut Rig, (u32, u32))) -> Rig {
    let mut rig = Rig::new(posts(kind, shape), &[SECRET], Mode::Held);
    let articles = rig.articles(serve);
    let bad = articles.len() / 2;
    let mut done = false;
    for (index, article) in articles.iter().copied().enumerate() {
        if index == bad {
            rig.intake_damaged(article);
        } else {
            rig.intake(article);
        }
        if !done && index >= bad + 2 && !rig.in_hand.is_empty() {
            rig.send_back(articles[bad]);
            then(&mut rig, articles[bad]);
            done = true;
        }
    }
    if !done {
        assert!(!rig.in_hand.is_empty(), "a loan is out at the send-back");
        rig.send_back(articles[bad]);
        then(&mut rig, articles[bad]);
    }
    rig.settle();
    rig
}

// As `sent_back`, but the loans come back before the good copy does.
fn sent_back_then_returned(kind: Kind, shape: Shape, serve: Serve) {
    let rig = send_back_with_a_loan_out(kind, shape, serve, |rig, bad| {
        rig.give_back();
        rig.intake(bad);
    });
    assert!(rig.delivered(), "the member delivered whole");
}

// The sent-back article's next copy never comes: every server failed it.
// Nothing rebuilds the member, and the job ends with nothing out.
fn sent_back_then_lost(kind: Kind, shape: Shape, serve: Serve) {
    let rig = send_back_with_a_loan_out(kind, shape, serve, |rig, bad| {
        rig.lost(bad);
        rig.give_back();
    });
    assert!(!rig.delivered(), "a member short an article is not whole");
}

// Every article in while a loan is out, then the bad one sent back with
// the loan still out, and its good copy after.
fn sent_back_after_the_last(kind: Kind, shape: Shape, serve: Serve) {
    let mut rig = Rig::new(posts(kind, shape), &[SECRET], Mode::Held);
    let articles = rig.articles(serve);
    let bad = articles.len() / 3;
    for (index, article) in articles.iter().copied().enumerate() {
        if index == bad {
            rig.intake_damaged(article);
        } else {
            rig.intake(article);
        }
    }
    assert!(!rig.in_hand.is_empty(), "a loan is out at the send-back");
    rig.send_back(articles[bad]);
    rig.intake(articles[bad]);
    rig.settle();
    assert!(rig.delivered(), "the member delivered whole");
}

macro_rules! rows {
    ($($name:ident: $body:expr;)*) => {
        $(
            #[test]
            fn $name() {
                $body
            }
        )*
    };
}

use At::{First, Late, Middle};
use Kind::{
    Rar4, Rar5, Rar5Sealed, SevenZipAes, ZipCryptoDescriptor as Described,
    ZipCryptoSized as Sized,
};
use Mode::{Held, Inline};
use Serve::{Backwards, InOrder, TailFirst};
use Shape::{Flat, Nested};

rows! {
    held_described_flat_in_order: served(Described, Flat, InOrder, &[SECRET]);
    held_described_flat_backwards: served(Described, Flat, Backwards, &[SECRET]);
    held_described_nested_in_order: served(Described, Nested, InOrder, &[SECRET]);
    held_described_nested_backwards: served(Described, Nested, Backwards, &[SECRET]);
    held_sized_flat_in_order: served(Sized, Flat, InOrder, &[SECRET]);
    held_sized_nested_backwards: served(Sized, Nested, Backwards, &[SECRET]);
    held_rar5_in_order: served(Rar5, Flat, InOrder, &[SECRET]);
    held_rar5_backwards: served(Rar5, Flat, Backwards, &[SECRET]);
    held_rar5_sealed_in_order: served(Rar5Sealed, Flat, InOrder, &[SECRET]);
    held_rar4_in_order: served(Rar4, Flat, InOrder, &[SECRET]);
    held_described_wrong_password_first: served(Described, Nested, InOrder, &["not-it", SECRET]);
    held_rar5_wrong_password_first: served(Rar5, Flat, InOrder, &["not-it", SECRET]);
    held_rar5_two_wrong_passwords_first: served(Rar5, Flat, Backwards, &["not-it", "nor-this", SECRET]);

    to_the_end_described_flat_in_order: held_to_the_end(Described, Flat, InOrder);
    to_the_end_described_nested_in_order: held_to_the_end(Described, Nested, InOrder);
    to_the_end_described_nested_backwards: held_to_the_end(Described, Nested, Backwards);
    to_the_end_sized_flat_backwards: held_to_the_end(Sized, Flat, Backwards);
    to_the_end_rar5_in_order: held_to_the_end(Rar5, Flat, InOrder);

    send_back_held_flat_in_order_first: sent_back(Described, Flat, InOrder, Held, First, 2);
    send_back_held_flat_in_order_middle: sent_back(Described, Flat, InOrder, Held, Middle, 2);
    send_back_held_flat_in_order_late: sent_back(Described, Flat, InOrder, Held, Late, 2);
    send_back_held_flat_in_order_at_once: sent_back(Described, Flat, InOrder, Held, Middle, 0);
    send_back_held_flat_backwards_first: sent_back(Described, Flat, Backwards, Held, First, 2);
    send_back_held_flat_backwards_middle: sent_back(Described, Flat, Backwards, Held, Middle, 2);
    send_back_held_flat_backwards_late: sent_back(Described, Flat, Backwards, Held, Late, 2);
    send_back_held_nested_in_order_first: sent_back(Described, Nested, InOrder, Held, First, 2);
    send_back_held_nested_in_order_middle: sent_back(Described, Nested, InOrder, Held, Middle, 2);
    send_back_held_nested_in_order_late: sent_back(Described, Nested, InOrder, Held, Late, 2);
    send_back_held_nested_in_order_far: sent_back(Described, Nested, InOrder, Held, First, 9);
    send_back_held_nested_backwards_first: sent_back(Described, Nested, Backwards, Held, First, 2);
    send_back_held_nested_backwards_middle: sent_back(Described, Nested, Backwards, Held, Middle, 2);
    send_back_held_nested_backwards_late: sent_back(Described, Nested, Backwards, Held, Late, 2);
    send_back_held_sized_nested_middle: sent_back(Sized, Nested, InOrder, Held, Middle, 2);
    send_back_held_sized_flat_middle: sent_back(Sized, Flat, InOrder, Held, Middle, 2);
    send_back_held_sized_flat_first: sent_back(Sized, Flat, InOrder, Held, First, 2);

    held_described_flat_tail_first: served(Described, Flat, TailFirst, &[SECRET]);
    held_described_nested_tail_first: served(Described, Nested, TailFirst, &[SECRET]);
    held_sized_flat_tail_first: served(Sized, Flat, TailFirst, &[SECRET]);
    held_sized_nested_tail_first: served(Sized, Nested, TailFirst, &[SECRET]);
    held_rar5_tail_first: served(Rar5, Flat, TailFirst, &[SECRET]);
    held_nested_tail_first_wrong_password_first: served(Described, Nested, TailFirst, &["not-it", SECRET]);
    to_the_end_described_nested_tail_first: held_to_the_end(Described, Nested, TailFirst);
    to_the_end_sized_flat_tail_first: held_to_the_end(Sized, Flat, TailFirst);
    send_back_held_tail_first_flat_first: sent_back(Described, Flat, TailFirst, Held, First, 2);
    send_back_held_tail_first_flat_middle: sent_back(Described, Flat, TailFirst, Held, Middle, 2);
    send_back_held_tail_first_flat_late: sent_back(Described, Flat, TailFirst, Held, Late, 2);
    send_back_held_tail_first_flat_at_once: sent_back(Described, Flat, TailFirst, Held, Middle, 0);
    send_back_held_tail_first_nested_first: sent_back(Described, Nested, TailFirst, Held, First, 2);
    send_back_held_tail_first_nested_middle: sent_back(Described, Nested, TailFirst, Held, Middle, 2);
    send_back_held_tail_first_nested_late: sent_back(Described, Nested, TailFirst, Held, Late, 2);
    send_back_held_tail_first_nested_far: sent_back(Described, Nested, TailFirst, Held, First, 9);
    send_back_held_tail_first_sized_flat_middle: sent_back(Sized, Flat, TailFirst, Held, Middle, 2);
    send_back_held_tail_first_sized_nested_middle: sent_back(Sized, Nested, TailFirst, Held, Middle, 2);
    send_back_inline_tail_first_nested_middle: sent_back(Described, Nested, TailFirst, Inline, Middle, 2);
    send_back_inline_tail_first_flat_first: sent_back(Described, Flat, TailFirst, Inline, First, 2);
    send_back_returned_tail_first_nested: sent_back_then_returned(Described, Nested, TailFirst);
    send_back_returned_tail_first_flat: sent_back_then_returned(Described, Flat, TailFirst);
    send_back_lost_tail_first_nested: sent_back_then_lost(Described, Nested, TailFirst);
    send_back_after_the_last_tail_first_nested: sent_back_after_the_last(Described, Nested, TailFirst);
    send_back_held_rar5_middle: sent_back(Rar5, Flat, InOrder, Held, Middle, 2);
    send_back_held_rar5_backwards_first: sent_back(Rar5, Flat, Backwards, Held, First, 2);

    send_back_inline_flat_in_order_first: sent_back(Described, Flat, InOrder, Inline, First, 2);
    send_back_inline_flat_in_order_middle: sent_back(Described, Flat, InOrder, Inline, Middle, 2);
    send_back_inline_flat_backwards_late: sent_back(Described, Flat, Backwards, Inline, Late, 2);
    send_back_inline_nested_in_order_middle: sent_back(Described, Nested, InOrder, Inline, Middle, 2);
    send_back_inline_nested_backwards_first: sent_back(Described, Nested, Backwards, Inline, First, 2);
    send_back_inline_rar5_middle: sent_back(Rar5, Flat, InOrder, Inline, Middle, 2);

    send_back_returned_flat_in_order: sent_back_then_returned(Described, Flat, InOrder);
    send_back_returned_nested_in_order: sent_back_then_returned(Described, Nested, InOrder);
    send_back_returned_nested_backwards: sent_back_then_returned(Described, Nested, Backwards);
    send_back_returned_rar5: sent_back_then_returned(Rar5, Flat, InOrder);

    send_back_lost_flat_in_order: sent_back_then_lost(Described, Flat, InOrder);
    send_back_lost_nested_backwards: sent_back_then_lost(Described, Nested, Backwards);
    send_back_lost_rar5: sent_back_then_lost(Rar5, Flat, InOrder);

    send_back_after_the_last_flat_in_order: sent_back_after_the_last(Described, Flat, InOrder);
    send_back_after_the_last_nested_in_order: sent_back_after_the_last(Described, Nested, InOrder);
    send_back_after_the_last_nested_backwards: sent_back_after_the_last(Described, Nested, Backwards);
    send_back_after_the_last_rar5: sent_back_after_the_last(Rar5, Flat, Backwards);

    held_sevenz_aes_in_order: served(SevenZipAes, Flat, InOrder, &[SECRET]);
    held_sevenz_aes_backwards: served(SevenZipAes, Flat, Backwards, &[SECRET]);
    held_sevenz_aes_tail_first: served(SevenZipAes, Flat, TailFirst, &[SECRET]);
    to_the_end_sevenz_aes_in_order: held_to_the_end(SevenZipAes, Flat, InOrder);
    send_back_held_sevenz_aes_middle: sent_back(SevenZipAes, Flat, InOrder, Held, Middle, 2);
    send_back_returned_sevenz_aes: sent_back_then_returned(SevenZipAes, Flat, InOrder);
    send_back_lost_sevenz_aes: sent_back_then_lost(SevenZipAes, Flat, InOrder);
    send_back_after_the_last_sevenz_aes: sent_back_after_the_last(SevenZipAes, Flat, InOrder);
}
