// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The first-article sample of one post, kept as its first articles settle
// or are lost. Each file's first ordinal, its bytes and whether it is
// recovery are read from the post once; after that a first article that
// changes costs one file's head, and reading the sample costs nothing of
// the post's size.

use crate::jobs::model::JobSpec;

use super::{Gate, Head, declared_recovery_bytes, declares_recovery, gate_of};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FirstFile {
    // The file's first ordinal, `None` for a file with no articles.
    first: Option<u32>,
    recovery: bool,
    bytes: u64,
    head: Head,
    // Waiting to have its head read again.
    stale: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::pipeline) struct FirstArticles {
    files: Vec<FirstFile>,
    // The files waiting to have their heads read again, each once.
    stale: Vec<u32>,
    // Of the payload files: how many, how many lost their first article,
    // and how many first articles are still to come.
    payload: usize,
    lost: usize,
    outstanding: usize,
    // The bytes of the payload files that lost their first article.
    lost_bytes: u64,
    // The bytes of the recovery files whose first article was not lost.
    reachable_bytes: u64,
    declared_recovery: u64,
}

impl FirstArticles {
    // The sample of the post `spec` names, each file's first article where
    // `head` says it stands.
    pub(in crate::pipeline) fn new(
        spec: &JobSpec,
        mut head: impl FnMut(u32, Option<u32>) -> Head,
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
        };
        for (file, index) in spec.files.iter().zip(0u32..) {
            let first = file.segments.iter().map(|segment| segment.ordinal).min();
            let recovery = declares_recovery(file);
            let entry = FirstFile {
                first,
                recovery,
                bytes: file
                    .segments
                    .iter()
                    .map(|segment| u64::from(segment.bytes))
                    .sum(),
                head: head(index, first),
                stale: false,
            };
            if !recovery {
                sample.payload += 1;
            }
            sample.count(entry, true);
            sample.files.push(entry);
        }
        sample
    }

    // Whether the sample was read from a post of `files` files.
    pub(in crate::pipeline) fn files(&self) -> usize {
        self.files.len()
    }

    // The bytes of every file the post names as recovery data.
    pub(in crate::pipeline) fn declared_recovery(&self) -> u64 {
        self.declared_recovery
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
        match (file.recovery, file.head) {
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

    // The bytes of the payload files that lost their first article, and of
    // the recovery files whose first article was not lost.
    pub(in crate::pipeline) fn weighed(&self) -> (u64, u64) {
        (self.lost_bytes, self.reachable_bytes)
    }
}
