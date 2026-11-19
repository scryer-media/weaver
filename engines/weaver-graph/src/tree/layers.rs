// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Release layers. The root's namespace is what was posted, and every
// container there is opened. Below it, inside what an archive held, a
// container is opened only when it is a layer of the release: its own name
// says it is an archive (`sniff::names_an_archive`), or the folder it sits
// in holds nothing but pieces of archive sets and furniture (recovery data,
// listings, text posted beside a payload, samples), as a scene release's
// zips, or a `CD1/` and `CD2/` of RAR volumes, are packed. Any other
// container below the root is a file the release holds, an application's
// data built on a zip or a compressed file of its own, and is delivered as
// posted: refused as `Why::Packed`, which disclaims it.
//
// A name decides at once, so a layer named as one streams. A folder decides
// only once nothing more can come into its namespace, at the job's quiet:
// until then a container no name opens is a leaf, its bytes written to its
// own file, and is noted in `unlayered`. At the quiet every namespace with
// one is read once, each member told by its name among the names beside it
// and by its front (`sniff::Listing::layered`), and the folders holding a
// file of their own are kept in `layers`. Each noted member is then asked
// again who owns it, and opens, or is refused, by its folder. Neither is
// recorded: a restart notes the members again as their fronts come back,
// and the quiet after it reads the same namespaces, so it decides the same.
//
// What a layer opened below the root holds lands in the folder the layer
// sat in, its own path joined to that folder's (`Tree::landing`); every
// name still passes the ledger's confinement (`names::confined`), so no
// part climbs out. What an archive at the root holds lands at the top, as
// it always has.

use std::collections::{BTreeMap, BTreeSet};

use super::{Builder, Effect, NodeId, Tree};
use crate::deliver::Why;
use crate::member::MemberId;
use crate::object::Kind;
use crate::sniff::{Layered, Listing, names_an_archive};

// The folders of one namespace that hold a file of their own, each by its
// key (`folder`).
pub(super) type Packed = BTreeSet<String>;

// The kinds the layer rule governs: containers a member can be opened as.
// Recovery data, listings and engines are not containers of the release.
fn governed(kind: Kind) -> bool {
    matches!(
        kind,
        Kind::RarStore
            | Kind::SevenZipStore
            | Kind::Zip
            | Kind::Tar
            | Kind::Xz
            | Kind::Gzip
            | Kind::Bzip2
            | Kind::Zstd
            | Kind::Brotli
            | Kind::Deflate
            | Kind::Split
    )
}

// The single-stream compressors. What one opens is one member, named for
// its volume with the compression taken off (its path kept), and nothing
// more ever joins it: a container there is the whole of its namespace.
fn stream(kind: Kind) -> bool {
    matches!(
        kind,
        Kind::Xz | Kind::Gzip | Kind::Bzip2 | Kind::Zstd | Kind::Brotli | Kind::Deflate
    )
}

// The folder a member's name sits in, its path up to the last separator,
// either separator read as `/`, in lower case: two members in one folder
// have one key.
fn folder(name: &str) -> String {
    let at = name.rfind(['/', '\\']).unwrap_or(0);
    name[..at].replace('\\', "/").to_ascii_lowercase()
}

impl Tree {
    // Whether `member`, founding an object of `kind` in `node`'s namespace,
    // may open as one. At the root, and for a kind the rule does not
    // govern, always; so too as the one member a stream opened, which is
    // every member its namespace will hold (a layer by its namespace, at
    // once). Below it, when its name says an archive, or when its
    // namespace's folders are known and its folder holds no file of its
    // own; refused as packed when they are known and its folder does. While
    // they are not known, it waits, a leaf, for the quiet.
    pub(super) fn opens_as_layer(
        &mut self,
        node: NodeId,
        member: MemberId,
        (kind, name): (Kind, &str),
        effects: &mut Vec<Effect>,
    ) -> bool {
        if !governed(kind)
            || self.depth(node) == 0
            || self.kind(node).is_some_and(stream)
            || names_an_archive(name, kind)
        {
            return true;
        }
        match self.layers.get(&node) {
            Some(packed) => {
                if !packed.contains(&folder(name)) {
                    return true;
                }
                effects.push(Effect::Refused {
                    node,
                    member,
                    why: Why::Packed,
                });
                false
            }
            None => {
                self.unlayered.insert((node, member));
                false
            }
        }
    }

    // Whether a container below the root waits for the quiet to say
    // whether it is a layer.
    pub fn layers_pending(&self) -> bool {
        !self.unlayered.is_empty()
    }

    // The job is quiet: nothing more comes into any namespace. Each one
    // with a container waiting is read once, its folders told, and every
    // waiting container is asked again who owns it, so it opens or is
    // refused as packed. What it opens may hold containers that wait in
    // turn, for the quiet after.
    pub fn decide_layers(&mut self, builder: &mut dyn Builder, effects: &mut Vec<Effect>) {
        let mut waiting: BTreeMap<NodeId, Vec<MemberId>> = BTreeMap::new();
        for (node, member) in std::mem::take(&mut self.unlayered) {
            waiting.entry(node).or_default().push(member);
        }
        for (node, members) in waiting {
            let Some(namespace) = self.namespace(node) else {
                continue;
            };
            let listing = Listing::new(namespace.members().map(|record| record.name.as_str()));
            let packed: Packed = namespace
                .members()
                .filter(|record| {
                    !record.name.ends_with(['/', '\\'])
                        && namespace.copy_of(record.id).is_none()
                        && listing.layered(
                            &record.name,
                            namespace.first_bytes(record.id),
                            record.length,
                        ) == Layered::Own
                })
                .map(|record| folder(&record.name))
                .collect();
            self.layers.insert(node, packed);
            for member in members {
                self.identify_again(node, member, builder, effects);
            }
        }
    }

    // The name a member wants on disk: its name in its namespace, under the
    // folder the container its namespace came out of sat in when that
    // container sat below the root. A member of the root's namespace, or of
    // a container posted at the root, wants its name as it is.
    pub(super) fn landing(&self, node: NodeId, member: MemberId) -> Option<String> {
        let name = &self.namespace(node)?.get(member)?.name;
        Some(match self.sat_in(node) {
            Some(folder) => format!("{folder}{name}"),
            None => name.clone(),
        })
    }

    // The folder, with its trailing separator, that the container `node`
    // opens sat in, where its first volume lands; `None` at the top, or for
    // a container posted at the root. A stream's member already carries its
    // volume's path, so it takes the folder of the namespace that volume
    // sits in, and a container a stream at the top opens sits at the top.
    fn sat_in(&self, node: NodeId) -> Option<String> {
        let binding = self.nodes.get(&node)?.binding.as_ref()?;
        if self.kind(node).is_some_and(stream) {
            return self.sat_in(binding.parent);
        }
        if self.at_top(binding.parent) {
            return None;
        }
        let first = *binding.volumes.values().next()?;
        let landing = self.landing(binding.parent, first)?;
        let at = landing.rfind(['/', '\\'])?;
        Some(landing[..=at].to_owned())
    }

    // Whether `node`'s namespace lands at the top: the root's, or a
    // stream's opened there.
    fn at_top(&self, node: NodeId) -> bool {
        node == NodeId::ROOT
            || (self.kind(node).is_some_and(stream)
                && self
                    .nodes
                    .get(&node)
                    .and_then(|found| found.binding.as_ref())
                    .is_some_and(|binding| self.at_top(binding.parent)))
    }
}

#[cfg(test)]
mod tests {
    use super::folder;

    #[test]
    fn a_folder_is_the_path_before_the_last_separator_in_lower_case() {
        assert_eq!(folder("ember.rar"), "");
        assert_eq!(folder("CD1/ember.r00"), "cd1");
        assert_eq!(folder("App\\Patch\\Patch.rar"), "app/patch");
        assert_eq!(folder("a/b/c.zip"), "a/b");
    }
}
