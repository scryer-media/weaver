// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The run for a set that stores some folders and codes others. The store
// streams the stored folders' members and keeps the coded folders' packs and
// the headers in its envelope, at their container offsets; the run reads
// that envelope as the container and decodes the coded folders alone, folder
// by folder, so no byte of a stored folder is ever read. It is the 7z
// engine's run, scoped to those folders, and its members are the store's,
// numbered as the store numbers them.

use weaver_graph::Extraction;

use super::map::Map;
use crate::sevenz_engine::run::{Run, Scope};

// With no map in hand there is no coded folder, and the run decodes nothing.
// `proven` when the store proved `password`.
pub(super) fn run(
    map: Option<&Map>,
    password: Option<String>,
    proven: bool,
) -> Box<dyn Extraction> {
    let folders: Vec<usize> = map.into_iter().flat_map(Map::coded).collect();
    let members = map
        .into_iter()
        .flat_map(|map| {
            folders
                .iter()
                .flat_map(|folder| map.folders[*folder].entries.iter().copied())
        })
        .collect();
    Box::new(Run::of(
        password,
        proven,
        Scope::Folders { folders, members },
    ))
}
