// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The PAR3 object's facts: the ranges of each volume that arrived and
// whether it closed; the described paths, whether a member carries each
// name, and the member each is bound to; the members that may be a free
// described file and the path a readback identified each as; the blocks
// the last repair found damaged while it waited; what that repair came to; and
// each set a repair settled, with the members it was bound to then.
//
// The kept fronts are not recorded: they are a cache of the volumes' first
// bytes, which the server's replay of the volumes delivers again, and the
// sets are read from them anew. A content match is kept as the described
// path it named, so a restore binds it again without reading the member
// back. Recovery blocks stay in the volumes and are read back when a repair
// spends them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use weaver_graph::{Coverage, Durable, Facts, Kind, MemberId, Object, ObjectId, RestoreError};

use super::{MAX_CARRIERS, Par3, Ruling, Settled, Unnamed, Volume};

#[derive(Serialize, Deserialize)]
pub(super) struct Par3Facts {
    id: ObjectId,
    pub(super) volumes: Vec<VolumeFacts>,
    pub(super) bound: BTreeMap<String, MemberId>,
    unnamed: BTreeMap<MemberId, Unnamed>,
    described: BTreeMap<String, bool>,
    pub(super) identified: BTreeMap<MemberId, String>,
    needed: u64,
    last: Option<Ruling>,
    settled: BTreeMap<String, Settled>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct VolumeFacts {
    arrived: Coverage,
    closed: bool,
}

impl Durable for Par3Facts {
    // 2: a repair that came up short kept with the volumes closed then.
    const VERSION: u16 = 2;

    fn impossible(&self) -> Option<&'static str> {
        if self.volumes.len() > MAX_CARRIERS {
            return Some("more volumes than one job carries");
        }
        if self
            .bound
            .keys()
            .any(|path| !self.described.contains_key(path))
        {
            return Some("a member bound to a path no set describes");
        }
        let identified = self.identified.iter().all(|(member, path)| {
            self.unnamed.contains_key(member) && self.described.contains_key(path)
        });
        (!identified).then_some("a member identified that could not be")
    }
}

pub(super) fn of(par3: &Par3) -> Facts {
    let facts = Par3Facts {
        id: par3.id,
        volumes: par3
            .volumes
            .iter()
            .map(|volume| VolumeFacts {
                arrived: volume.arrived.clone(),
                closed: volume.closed,
            })
            .collect(),
        bound: par3.bound.clone(),
        unnamed: par3.unnamed.clone(),
        described: par3.described.clone(),
        identified: par3.identified.clone(),
        needed: par3.needed,
        last: par3.last,
        settled: par3.settled.clone(),
    };
    Facts::of(Kind::Par3, &facts)
}

pub(super) fn par3(facts: &Facts) -> Result<Par3, RestoreError> {
    let kept: Par3Facts = facts.read(Kind::Par3)?;
    let mut par3 = Par3::new(kept.id);
    par3.volumes = kept
        .volumes
        .into_iter()
        .map(|volume| Volume {
            front: Vec::new(),
            arrived: volume.arrived,
            closed: volume.closed,
            named_blocks: None,
        })
        .collect();
    par3.bound = kept.bound;
    par3.unnamed = kept.unnamed;
    par3.described = kept.described;
    par3.identified = kept.identified;
    par3.needed = kept.needed;
    par3.last = kept.last;
    par3.settled = kept.settled;
    Ok(par3)
}

pub(crate) fn restore(facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
    Ok(Box::new(par3(facts)?))
}
