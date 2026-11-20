// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph engine: every job's articles go from the decoder through the
// root, the tree and the namespace writer to its files.

pub mod checkpoint;
pub(crate) mod decoding;
pub(crate) mod disk;
pub mod engine;
pub(crate) mod envelope;
pub(crate) mod hardware;
pub mod job;
pub mod leaf;
pub mod limits;
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub mod lines;
pub(crate) mod metadata;
pub mod namespace;
pub mod restore;
pub mod root;
pub mod status;
pub(crate) mod summary;
pub(crate) mod window;

// The one priority every article of a graph job is queued at. The root is
// the only ranker: its classes raise what it wants sooner, and nothing about
// a file's name or role orders the queue, so the posted order breaks every
// tie. It sits behind the window volumes the root raises.
pub(crate) const NEUTRAL_PRIORITY: u32 = 10;

#[cfg(test)]
mod tests;
