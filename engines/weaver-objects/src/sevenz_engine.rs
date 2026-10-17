// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The 7z engine: conventional extraction of a 7z set on disk, whole or
// still growing, through sevenz-turbo. The run and the naming are its own;
// the object is the shared engine object.

pub mod naming;
mod run;

#[cfg(test)]
use weaver_graph::{ByteRange, Emit, Engine, MemberId, Object, Placement, VolumeIndex};
use weaver_graph::{Extraction, Kind};

use crate::engine::{EngineObject, Format};

pub struct SevenZip;

impl Format for SevenZip {
    const KIND: Kind = Kind::SevenZipEngine;
    type Name = naming::VolumeName;

    fn parse(name: &str) -> Option<naming::VolumeName> {
        naming::parse(name)
    }

    fn run(password: Option<String>) -> Box<dyn Extraction> {
        Box::new(run::Run::new(password))
    }
}

pub type SevenZipEngine = EngineObject<SevenZip>;

#[cfg(test)]
mod tests;
