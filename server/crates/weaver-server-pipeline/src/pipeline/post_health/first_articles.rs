// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The first-article sample of one post, kept as its first articles settle
// or are lost. Each file's first ordinal, its bytes and whether it is
// recovery are read from the post once; after that a first article that
// changes costs one file's head, and reading the sample costs nothing of
// the post's size.
//
// Beside the heads it keeps which files the graph holds as recovery that
// the post's names do not declare: the graph's answer, read for the members
// whose rows changed since it was last read, so a binding costs its member
// and the critical line follows it without a walk of the post. The gate
// weighs a file the names do not count by the same answer: one the graph
// holds as recovery is recovery to it, as a named volume is. A named file
// whose front the graph read and found no recovery data in counts by its
// bytes, not its name, from then on: payload, unless the graph holds it.

use crate::jobs::model::JobSpec;

use super::{Gate, Head, declared_recovery_bytes, declares_recovery, gate_of, names_recovery};

fn dead_of(gate: Gate, lost: u64, reachable: u64) -> Option<(usize, usize)> {
    match gate {
        Gate::Dead { missing, total } if lost > reachable => Some((missing, total)),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FirstFile {
    // The file's first ordinal, `None` for a file with no articles.
    first: Option<u32>,
    // The post's names declare it recovery data: a PAR2 or PAR3 volume, or
    // a RAR recovery volume. A PAR2 or PAR3 index is not.
    recovery: bool,
    // The post's names count it in the critical line: a PAR2 or PAR3 file,
    // its index among them, or a RAR recovery volume.
    named: bool,
    // The graph holds it as a recovery set's volume.
    bound: bool,
    // The graph read its front, which opens no recovery format: its name
    // counts for nothing (`GraphJob::front_refutes_recovery`).
    refuted: bool,
    bytes: u64,
    // The bytes of it every server said are gone.
    lost: u64,
    head: Head,
    // Waiting to have its head read again.
    stale: bool,
}

impl FirstFile {
    // Whether the first-article gate weighs the file as recovery: a file
    // the names count, by what its name declares; any other, by what the
    // graph holds it as.
    fn gate_recovery(&self) -> bool {
        if self.named() {
            self.recovery
        } else {
            self.bound
        }
    }

    // Whether the names count it, its front not saying otherwise.
    fn named(&self) -> bool {
        self.named && !self.refuted
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::pipeline) struct FirstArticles {
    files: Vec<FirstFile>,
    // The files waiting to have their heads read again, each once.
    stale: Vec<u32>,
    // Of the payload files, every file the gate does not weigh as
    // recovery: how many, how many lost their first article, and how many
    // first articles are still to come.
    payload: usize,
    lost: usize,
    outstanding: usize,
    // The bytes of the payload files that lost their first article.
    lost_bytes: u64,
    // The bytes of the files the gate weighs as recovery whose first
    // article was not lost.
    reachable_bytes: u64,
    declared_recovery: u64,
    // The bytes of the files the graph holds as recovery that the names do
    // not count.
    bound_bytes: u64,
    // The bytes of the files the names count whose fronts said otherwise.
    refuted_bytes: u64,
    // The files whose first article is still to come and which nothing
    // counts as recovery yet: each may still bind as a recovery volume,
    // and bring what of it is not gone.
    unresolved: usize,
    unresolved_reach: u64,
    // The generation of the root's members the bindings were read at.
    bindings_at: Option<u64>,
}

impl FirstArticles {
    // The sample of the post `spec` names, each file's first article where
    // `head` says it stands, and each file's bytes every server said are
    // gone where `lost` says.
    pub(in crate::pipeline) fn new(
        spec: &JobSpec,
        mut head: impl FnMut(u32, Option<u32>) -> Head,
        lost: impl Fn(u32) -> u64,
    ) -> Self {
        let mut sample = Self {
            files: Vec::with_capacity(spec.files.len()),
            stale: Vec::new(),
            payload: 0,
            lost: 0,
            outstanding: 0,
            lost_bytes: 0,
            reachable_bytes: 0,
            declared_recovery: declared_recovery_bytes(spec),
            bound_bytes: 0,
            refuted_bytes: 0,
            unresolved: 0,
            unresolved_reach: 0,
            bindings_at: None,
        };
        for (file, index) in spec.files.iter().zip(0u32..) {
            let first = file.segments.iter().map(|segment| segment.ordinal).min();
            let entry = FirstFile {
                first,
                recovery: declares_recovery(file),
                named: names_recovery(file),
                bound: false,
                refuted: false,
                bytes: file
                    .segments
                    .iter()
                    .map(|segment| u64::from(segment.bytes))
                    .sum(),
                lost: lost(index),
                head: head(index, first),
                stale: false,
            };
            sample.count(entry, true);
            sample.files.push(entry);
        }
        sample
    }

    // Whether the sample was read from a post of `files` files.
    pub(in crate::pipeline) fn files(&self) -> usize {
        self.files.len()
    }

    // The recovery bytes the critical line is drawn from: what the post's
    // names declare, and every file the graph holds as recovery besides.
    pub(in crate::pipeline) fn recovery(&self) -> u64 {
        self.declared_recovery
            .saturating_sub(self.refuted_bytes)
            .saturating_add(self.bound_bytes)
    }

    // How many files have a first article still to come that may yet bind
    // as recovery.
    pub(in crate::pipeline) fn unresolved(&self) -> usize {
        self.unresolved
    }

    // What those files would bring were every one of them a recovery
    // volume: what of each is not gone.
    pub(in crate::pipeline) fn unresolved_reach(&self) -> u64 {
        self.unresolved_reach
    }

    // Every server said `bytes` more of the file at `index` are gone.
    pub(in crate::pipeline) fn lose(&mut self, index: u32, bytes: u64) {
        let Some(file) = self.files.get(index as usize).copied() else {
            return;
        };
        self.count(file, false);
        let file = FirstFile {
            lost: file.lost.saturating_add(bytes),
            ..file
        };
        self.count(file, true);
        self.files[index as usize] = file;
    }

    // The generation of the root's members the bindings were last read at.
    pub(in crate::pipeline) fn bindings_at(&self) -> Option<u64> {
        self.bindings_at
    }

    pub(in crate::pipeline) fn set_bindings_at(&mut self, at: u64) {
        self.bindings_at = Some(at);
    }

    // Whether the graph holds the file at `index` as recovery now, and
    // whether its front said it is none.
    pub(in crate::pipeline) fn bind(&mut self, index: u32, bound: bool, refuted: bool) {
        let Some(file) = self.files.get(index as usize).copied() else {
            return;
        };
        if file.bound == bound && file.refuted == refuted {
            return;
        }
        self.count(file, false);
        let file = FirstFile {
            bound,
            refuted,
            ..file
        };
        self.count(file, true);
        self.files[index as usize] = file;
    }

    // The file's first ordinal.
    pub(in crate::pipeline) fn first(&self, index: u32) -> Option<u32> {
        self.files.get(index as usize).and_then(|file| file.first)
    }

    // An article of the file at `index` was given up: its head is read
    // again when it is the file's first.
    pub(in crate::pipeline) fn touch(&mut self, index: u32, ordinal: u32) {
        if self.first(index) == Some(ordinal) {
            self.stale(index);
        }
    }

    // The file's head is read again before the sample is.
    pub(in crate::pipeline) fn stale(&mut self, index: u32) {
        if let Some(file) = self.files.get_mut(index as usize)
            && !file.stale
        {
            file.stale = true;
            self.stale.push(index);
        }
    }

    // The files whose heads are to be read again, each once.
    pub(in crate::pipeline) fn take_stale(&mut self) -> Vec<u32> {
        let stale = std::mem::take(&mut self.stale);
        for index in &stale {
            self.files[*index as usize].stale = false;
        }
        stale
    }

    // The file's first article stands at `head` now.
    pub(in crate::pipeline) fn set(&mut self, index: u32, head: Head) {
        let Some(file) = self.files.get(index as usize).copied() else {
            return;
        };
        if file.head == head {
            return;
        }
        self.count(file, false);
        let file = FirstFile { head, ..file };
        self.count(file, true);
        self.files[index as usize] = file;
    }

    fn count(&mut self, file: FirstFile, add: bool) {
        let step = |count: &mut usize| {
            *count = if add { *count + 1 } else { *count - 1 };
        };
        let bytes = |sum: &mut u64| {
            *sum = if add {
                *sum + file.bytes
            } else {
                *sum - file.bytes
            };
        };
        if file.bound && !file.named() {
            bytes(&mut self.bound_bytes);
        }
        if file.named && file.refuted {
            bytes(&mut self.refuted_bytes);
        }
        if file.head == Head::Outstanding && !file.bound && !file.named() {
            step(&mut self.unresolved);
            let reach = file.bytes.saturating_sub(file.lost);
            self.unresolved_reach = if add {
                self.unresolved_reach + reach
            } else {
                self.unresolved_reach - reach
            };
        }
        let recovery = file.gate_recovery();
        if !recovery {
            step(&mut self.payload);
        }
        match (recovery, file.head) {
            (false, Head::Lost) => {
                step(&mut self.lost);
                bytes(&mut self.lost_bytes);
            }
            (false, Head::Outstanding) => step(&mut self.outstanding),
            (false, Head::Settled) => {}
            (true, Head::Lost) => {}
            (true, _) => bytes(&mut self.reachable_bytes),
        }
    }

    // The sample's verdict on the payload files.
    pub(in crate::pipeline) fn gate(&self) -> Gate {
        gate_of(self.payload, self.lost, self.outstanding)
    }

    // The gate's word that the post is dead, as how many of how many payload
    // files lost their first article: they reach the share, and the bytes
    // of those files outweigh the recovery whose first article was not lost.
    pub(in crate::pipeline) fn dead(&self) -> Option<(usize, usize)> {
        dead_of(
            gate_of(self.payload, self.lost, self.outstanding),
            self.lost_bytes,
            self.reachable_bytes,
        )
    }

    // The same word were every file that may yet bind a recovery volume:
    // each leaves the payload and brings what of it is not gone.
    pub(in crate::pipeline) fn dead_at_reach(&self) -> bool {
        dead_of(
            gate_of(
                self.payload - self.unresolved,
                self.lost,
                self.outstanding - self.unresolved,
            ),
            self.lost_bytes,
            self.reachable_bytes + self.unresolved_reach,
        )
        .is_some()
    }
}
