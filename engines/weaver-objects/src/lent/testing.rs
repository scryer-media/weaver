// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A lender for tests: heap memory filled with garbage, every ask recorded,
// every window counted until it is dropped, and a refusal of a chosen kind
// on demand.

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use weaver_graph::{Budget, WindowLender, WindowMemory};

// What every lent byte holds before the decoder writes it: a decoder that
// read a byte it had not written would decode something else.
pub(crate) const GARBAGE: u8 = 0xA5;

#[derive(Default)]
pub(crate) struct TestLender {
    asked: Mutex<Vec<usize>>,
    live: Arc<AtomicUsize>,
    peak: AtomicUsize,
    refuse: Option<io::ErrorKind>,
    // How many asks are lent before the refusals begin.
    lends_first: usize,
    ceiling: Option<u64>,
}

impl TestLender {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    // A lender that refuses every window with an error of `kind`.
    pub(crate) fn refusing(kind: io::ErrorKind) -> Arc<Self> {
        Arc::new(Self {
            refuse: Some(kind),
            ..Self::default()
        })
    }

    // A lender that lends the first `lends` windows asked for and refuses
    // every later one with an error of `kind`.
    pub(crate) fn refusing_after(lends: usize, kind: io::ErrorKind) -> Arc<Self> {
        Arc::new(Self {
            refuse: Some(kind),
            lends_first: lends,
            ..Self::default()
        })
    }

    // A lender whose ceiling is `ceiling`.
    pub(crate) fn up_to(ceiling: u64) -> Arc<Self> {
        Arc::new(Self {
            ceiling: Some(ceiling),
            ..Self::default()
        })
    }

    // The windows asked for, in the order asked, refused ones too.
    pub(crate) fn asked(&self) -> Vec<usize> {
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    // The windows lent and not yet dropped.
    pub(crate) fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    // The most windows lent at once.
    pub(crate) fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    // `budget`, lending through this lender.
    pub(crate) fn lending(self: &Arc<Self>, budget: Budget) -> Budget {
        Budget {
            lender: Some(Arc::clone(self) as Arc<dyn WindowLender>),
            ..budget
        }
    }
}

impl WindowLender for TestLender {
    fn lend(&self, len: usize) -> io::Result<Box<dyn WindowMemory>> {
        let mut asked = self.asked.lock().unwrap_or_else(PoisonError::into_inner);
        asked.push(len);
        if let Some(kind) = self.refuse.filter(|_| asked.len() > self.lends_first) {
            return Err(io::Error::new(kind, "test lender refuses"));
        }
        drop(asked);
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
        Ok(Box::new(Garbage {
            bytes: vec![GARBAGE; len].into_boxed_slice(),
            live: Arc::clone(&self.live),
        }))
    }

    fn ceiling(&self) -> u64 {
        self.ceiling.unwrap_or(64 << 30)
    }
}

struct Garbage {
    bytes: Box<[u8]>,
    live: Arc<AtomicUsize>,
}

impl Drop for Garbage {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

// SAFETY: the bytes are a heap allocation of `len` initialized bytes this
// value alone owns; it is never resized, so its address holds until drop,
// also when the value moves, and nothing else reaches it.
unsafe impl WindowMemory for Garbage {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.bytes.as_mut_ptr()
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }
}
