// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A stand-in until the archive stores stream: an archive the sniffer names as
// a store is built as its engine instead, so every archive is read, if not
// yet streamed. The store objects replace it as they land, the 7z store in
// phase 4 and the RAR store in phase 5, and this file goes with the second.
// Where the objects have no engine for a kind yet, the build finds nothing and
// the member stays a leaf, as before.

use weaver_graph::{Builder, Kind, Object, ObjectId};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct EnginesForStores<B>(pub(crate) B);

impl<B: Builder> Builder for EnginesForStores<B> {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        self.0.build(kind.engine_for().unwrap_or(kind), id)
    }
}
