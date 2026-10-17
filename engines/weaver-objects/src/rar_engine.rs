// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The RAR engine: conventional extraction of a RAR set on disk, whole or
// still growing, through unrar-rs. The run and the naming are its own; the
// object is the shared engine object.

pub mod naming;
mod run;

#[cfg(test)]
use weaver_graph::{ByteRange, Emit, Engine, Object, Placement, VolumeIndex};
use weaver_graph::{Extraction, Kind};

use crate::engine::{EngineObject, Format};

pub struct Rar;

impl Format for Rar {
    const KIND: Kind = Kind::RarEngine;
    type Name = naming::VolumeName;

    fn parse(name: &str) -> Option<naming::VolumeName> {
        naming::parse(name)
    }

    fn run(password: Option<String>) -> Box<dyn Extraction> {
        Box::new(run::Run::new(password))
    }
}

pub type RarEngine = EngineObject<Rar>;

#[cfg(test)]
mod tests;
