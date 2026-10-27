// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Passwords. An encrypted end header proves a password by parsing; a plain
// header over encrypted folders proves it by one file's checksum, since a
// 7z folder carries no password check of its own. The file is the smallest
// one with a checksum, so the proof costs the fewest bytes; its cipher
// blocks are kept, with the block before them as their seed, until a
// candidate deciphers them into the checksum the header wrote. With no such
// file the first candidate left is admitted unproven, on trial: a file whose
// checksum fails on bytes the tree verified condemns it, every folder it
// keyed asks for its bytes again, and they open under the next candidate as
// they come back. A checksum that fails on bytes not verified is damage, as
// under a proved candidate: repair runs first, and the trial goes on over
// the repaired bytes. Every candidate condemned leaves the store asking.

use std::sync::Arc;

use sevenz_turbo::AesProperties;
use weaver_graph::{ByteRange, Emit, Why};
use zeroize::Zeroizing;

use super::header::{self, Start};
use super::map::Map;
use super::{Lock, SevenZipStore};
use crate::crypt::{self, BLOCK, Cbc, Padding};
use crate::grid::crc32;
use crate::keys::{Derived, Tuple};

// The largest file a password is proved against. Its cipher bytes are kept
// until a candidate is proved, on top of whatever the owner holds.
pub(super) const PROOF_CAP: u64 = 4 * 1024 * 1024;

const FOLDER_TAG: &str = "7z-aes";
const HEADER_TAG: &str = "7z-header";

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub(super) struct Proof {
    folder: usize,
    entry: usize,
    // The file's cipher blocks in container coordinates, from its seed.
    pub(super) range: ByteRange,
    // Whether the range starts with the seed block, or the folder's IV seeds it.
    seeded: bool,
}

impl Proof {
    // Whether it names a file of this map, as a restored proof must.
    pub(super) fn fits(&self, map: &Map) -> bool {
        self.folder < map.folders.len() && self.entry < map.entries.len()
    }

    pub(super) fn choose(map: &Map) -> Option<Proof> {
        let (entry, folder) = map
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                entry.length > 0 && entry.length <= PROOF_CAP && entry.crc.is_some()
            })
            .filter_map(|(index, entry)| {
                let folder = entry.folder?;
                map.folders[folder].aes?;
                Some((index, folder))
            })
            .min_by_key(|(index, _)| (map.entries[*index].length, *index))?;
        let span = map.entries[entry].span();
        let first = span.start - span.start % BLOCK;
        let seeded = first > 0;
        let start = if seeded { first - BLOCK } else { 0 };
        let end = span.end.div_ceil(BLOCK) * BLOCK;
        let pack = map.folders[folder].pack.start;
        Some(Proof {
            folder,
            entry,
            range: ByteRange::new(pack + start, pack + end),
            seeded,
        })
    }
}

enum Verdict {
    Waiting,
    Right,
    Wrong,
    Unproven,
    Failed,
}

impl SevenZipStore {
    // Ask the job for a password, once until an offer answers it: asking
    // again before then would only ask the same question.
    pub(super) fn ask(&mut self, out: &mut Vec<Emit>) {
        if !self.asked {
            self.asked = true;
            out.push(Emit::NeedsPassword);
        }
    }

    pub(super) fn open_header(&mut self, start: Start, out: &mut Vec<Emit>) {
        let total = start.total();
        for index in 0..self.candidates.len() {
            if self.candidates[index].header_wrong {
                continue;
            }
            if self
                .header_tuple(index, start)
                .is_some_and(|tuple| self.keys.is_wrong(&tuple))
            {
                self.candidates[index].header_wrong = true;
                continue;
            }
            let parsed = header::parse(
                &self.kept,
                total,
                Some(&self.candidates[index].password),
                true,
                &super::limits(&self.budget),
            );
            match parsed {
                header::Parsed::Missing(range) => return self.want_header(range),
                header::Parsed::Unverified => return,
                header::Parsed::Locked | header::Parsed::Broken => {
                    if let Some(tuple) = self.header_tuple(index, start) {
                        self.keys.mark_wrong(&tuple);
                    }
                    self.candidates[index].header_wrong = true;
                }
                header::Parsed::Archive { archive, read } => {
                    self.opened = Some(index);
                    self.proven = true;
                    return self.build(&archive, &read, out);
                }
            }
        }
        self.ask(out);
    }

    // The key the cache files this candidate under for this header, once
    // the header is in hand. The header's own bytes stand for its salt:
    // sevenz-turbo derives the header's key inside its parse.
    fn header_tuple(&self, index: usize, start: Start) -> Option<Tuple<'_>> {
        Some(Tuple {
            format: HEADER_TAG,
            password: &self.candidates[index].secret,
            salt: &[],
            params: self.kept.read(start.end_header)?,
        })
    }

    pub(super) fn open_folders(&mut self, out: &mut Vec<Emit>) {
        if !self.folders.iter().any(Lock::is_locked) {
            return;
        }
        if let Some(index) = self.opened {
            return self.open_all(index, out);
        }
        for index in 0..self.candidates.len() {
            if self.candidates[index].folder_wrong {
                continue;
            }
            match self.prove(index) {
                Verdict::Waiting => return,
                verdict @ (Verdict::Right | Verdict::Unproven) => {
                    self.opened = Some(index);
                    self.proven = matches!(verdict, Verdict::Right);
                    return self.open_all(index, out);
                }
                Verdict::Wrong => self.candidates[index].folder_wrong = true,
                Verdict::Failed => return self.give_up(Emit::NeedsEngine(Why::Unsupported), out),
            }
        }
        self.ask(out);
    }

    // Whether the folders are open under a candidate nothing proved.
    pub(super) fn on_trial(&self) -> bool {
        self.opened.is_some() && !self.proven
    }

    // The candidate on trial is wrong: the next one is tried on the same
    // bytes, which every folder it opened asks for again. The folders stay
    // as they are until those bytes are said again, so what was posted reads
    // back as it was posted.
    pub(super) fn condemn(&mut self, out: &mut Vec<Emit>) {
        let Some(index) = self.opened.take() else {
            return;
        };
        self.candidates[index].folder_wrong = true;
        let Some(map) = self.map.clone() else {
            return;
        };
        let mut pieces = Vec::new();
        for (at, folder) in map.folders.iter().enumerate() {
            if !matches!(self.folders[at], Lock::Open(..)) {
                continue;
            }
            self.condemned.insert(at);
            self.to_volumes(folder.pack, &mut pieces);
        }
        out.extend(
            pieces
                .into_iter()
                .map(|(volume, range)| Emit::Again { volume, range }),
        );
    }

    fn folder_tuple<'a>(
        &'a self,
        index: usize,
        properties: &'a AesProperties,
        params: &'a [u8],
    ) -> Tuple<'a> {
        Tuple {
            format: FOLDER_TAG,
            password: &self.candidates[index].secret,
            salt: properties.salt(),
            params,
        }
    }

    // The key for a folder, through the process's cache and sevenz-turbo's
    // own derivation. `Ok(None)` when the cache knows the candidate is wrong.
    fn folder_key(
        &self,
        index: usize,
        properties: &AesProperties,
    ) -> Result<Option<Zeroizing<[u8; 32]>>, sevenz_turbo::Error> {
        let params = [properties.cycles_power()];
        let tuple = self.folder_tuple(index, properties, &params);
        let password = &self.candidates[index].password;
        let derived = self.keys.derive(&tuple, || {
            properties
                .derive_key(password, &super::limits(&self.budget))
                .map(|key| Zeroizing::new(key.as_bytes().to_vec()))
        })?;
        let Derived::Key(bytes) = derived else {
            return Ok(None);
        };
        let mut key = Zeroizing::new([0u8; 32]);
        if bytes.len() != key.len() {
            return Err(sevenz_turbo::Error::Other(
                "a derived key of the wrong length".into(),
            ));
        }
        key.copy_from_slice(&bytes);
        Ok(Some(key))
    }

    // A store that proved a candidate before a restart is offered that one
    // again, and it stands proved: the proof's bytes are gone to their
    // members, and nothing else would prove it.
    fn prove(&self, index: usize) -> Verdict {
        if self.proven {
            return Verdict::Right;
        }
        let Some(proof) = self.proof else {
            return Verdict::Unproven;
        };
        let Some(map) = &self.map else {
            return Verdict::Waiting;
        };
        let (Some(properties), Some(expected)) =
            (map.folders[proof.folder].aes, map.entries[proof.entry].crc)
        else {
            return Verdict::Failed;
        };
        // Damaged bytes would fail every candidate: wait for their repair.
        let Some(bytes) = self.kept.read(proof.range) else {
            return Verdict::Waiting;
        };
        if !self.kept.verified(proof.range) {
            return Verdict::Waiting;
        }
        let key = match self.folder_key(index, &properties) {
            Ok(Some(key)) => key,
            Ok(None) => return Verdict::Wrong,
            Err(_) => return Verdict::Failed,
        };
        let (seed, blocks) = if proof.seeded {
            let mut seed = [0; 16];
            seed.copy_from_slice(&bytes[..BLOCK as usize]);
            (seed, &bytes[BLOCK as usize..])
        } else {
            (*properties.iv(), bytes)
        };
        let mut plain = blocks.to_vec();
        if crypt::decipher(&key[..], seed, &mut plain).is_err() {
            return Verdict::Failed;
        }
        let entry = &map.entries[proof.entry];
        let from = (entry.offset % BLOCK) as usize;
        // A checksum that fails rules the candidate wrong for these bytes,
        // not for its key: the cache keeps the key, and another job proves
        // it again on bytes of its own.
        if crc32(&plain[from..from + entry.length as usize]) == expected {
            Verdict::Right
        } else {
            Verdict::Wrong
        }
    }

    fn open_all(&mut self, index: usize, out: &mut Vec<Emit>) {
        let Some(map) = self.map.take() else {
            return;
        };
        let mut failed = false;
        for (at, folder) in map.folders.iter().enumerate() {
            let Some(properties) = folder.aes.filter(|_| self.folders[at].is_locked()) else {
                continue;
            };
            let lock = std::mem::replace(&mut self.folders[at], Lock::Locked);
            let opened = match (self.folder_key(index, &properties), lock) {
                // Sealed at a restart: the chain it kept, keyed again.
                (Ok(Some(key)), Lock::Sealed(chain, padding)) => {
                    Cbc::unseal(&key[..], *chain).ok().map(|cbc| (cbc, padding))
                }
                (Ok(Some(key)), _) => Cbc::new(
                    &key[..],
                    *properties.iv(),
                    folder.pack.len(),
                    folder.plain_len,
                )
                .ok()
                .map(|cbc| (cbc, Padding::default())),
                (Ok(None) | Err(_), _) => None,
            };
            match opened {
                Some((cbc, padding)) => {
                    self.folders[at] = Lock::Open(Arc::new(cbc), Arc::new(padding));
                }
                None => failed = true,
            }
        }
        self.map = Some(map);
        if failed {
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        }
        // The proof's bytes were kept, not held: they go to their members
        // here, and the owner holds the rest of the folder's. Nothing of a
        // folder opened only now has come out, so there is nothing to read.
        if let Some(proof) = self.proof.take() {
            for (range, bytes, provenance) in self.kept.take(proof.range) {
                self.route(range, &bytes, provenance, &mut |_, _| false, out);
            }
        }
    }
}
