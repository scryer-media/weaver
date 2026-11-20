// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph engine: every job's articles go from the decoder through the
// root, the tree and the namespace writer to its files.

pub mod checkpoint;
pub mod decoding;
pub(crate) mod disk;
pub mod engine;
pub mod envelope;
pub mod hardware;
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
pub const NEUTRAL_PRIORITY: u32 = 10;

// What the unit tests compiled apart in tests/pipeline_graph.rs reach in
// this module's private modules.
#[cfg(feature = "pipeline-test-fixtures")]
#[doc(hidden)]
pub mod units {
    pub mod decoding {
        pub use super::super::decoding::*;
    }
    pub mod disk {
        pub use super::super::disk::*;
    }
    pub mod envelope {
        pub use super::super::envelope::*;
    }
    pub mod envelope_durable {
        pub use super::super::envelope::durable::*;
    }
    pub mod metadata {
        pub use super::super::metadata::*;
    }
    pub mod window {
        pub use super::super::window::*;
    }
}
