// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The zip engine: conventional extraction of a zip on disk, whole or
// spanned, still growing, through the zip library. The run and the naming
// are its own; the object is the shared engine object.

mod image;
pub mod naming;
mod run;

use weaver_graph::{Extraction, Kind};

use crate::engine::{EngineObject, Format};

// The directory is rewritten and decoded into entry metadata before any
// member is emitted. The run reserves this much ahead of it; a directory
// takes what its own bytes take, however large.
const ZIP_RUN_WINDOW: u64 = 60 << 20;

pub struct ZipFormat;

impl Format for ZipFormat {
    const KIND: Kind = Kind::ZipEngine;
    type Name = naming::VolumeName;
    type Front = ();

    fn parse(name: &str) -> Option<naming::VolumeName> {
        naming::parse(name)
    }

    fn run(password: Option<String>) -> Box<dyn Extraction> {
        Box::new(run::Run::new(password))
    }

    fn reserve() -> u64 {
        ZIP_RUN_WINDOW
    }
}

pub type ZipEngine = EngineObject<ZipFormat>;

#[cfg(test)]
mod tests;
