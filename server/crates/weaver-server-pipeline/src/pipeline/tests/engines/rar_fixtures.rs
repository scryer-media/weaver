// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{Post, par2_set};

pub fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

pub fn posts(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    volumes
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect()
}

// The volumes, then an index and a volume of two recovery blocks over them.
pub fn with_par2(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let set = par2_set("silver.horizon", &files, 2);
    let mut out = posts(volumes.clone());
    out.extend(set);
    out
}
