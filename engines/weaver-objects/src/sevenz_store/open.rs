// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Passwords. An encrypted end header proves a password by parsing; a plain
// header over encrypted folders proves it by one file's checksum, since a
// 7z folder carries no password check of its own. The file is the smallest
// one with a checksum, so the proof costs the fewest bytes; its cipher
// blocks are kept, with the block before them as their seed, until a
// candidate deciphers them into the checksum the header wrote. With no such
// file the first candidate is admitted unproven, and a checksum that fails
// later is damage like any other.

use sevenz_turbo::{AesProperties, ArchiveLimits};
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
const PROOF_CAP: u64 = 4 * 1024 * 1024;

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
    // Ask the job for a password, once: an offer answers it, and asking
    // again would only ask the same question.
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
            if self.header_wrong(index, start, false) {
                self.candidates[index].header_wrong = true;
                continue;
            }
            let parsed = header::parse(
                &self.kept,
                total,
                Some(&self.candidates[index].password),
                true,
            );
            match parsed {
                header::Parsed::Missing(range) => return self.want_header(range),
                header::Parsed::Unverified => return,
                header::Parsed::Locked | header::Parsed::Broken => {
                    self.header_wrong(index, start, true);
                    self.candidates[index].header_wrong = true;
                }
                header::Parsed::Archive { archive, read } => {
                    self.opened = Some(index);
                    return self.build(&archive, &read, out);
                }
            }
        }
        self.ask(out);
    }

    // Whether the cache knows this candidate is wrong for this header, or,
    // with `mark`, tell it so. The header's own bytes stand for its salt:
    // sevenz-turbo derives the header's key inside its parse.
    fn header_wrong(&self, index: usize, start: Start, mark: bool) -> bool {
        let Some(params) = self.kept.read(start.end_header) else {
            return false;
        };
        let tuple = Tuple {
            format: HEADER_TAG,
            password: &self.candidates[index].secret,
            salt: &[],
            params,
        };
        if mark {
            self.keys.mark_wrong(&tuple);
        }
        self.keys.is_wrong(&tuple)
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
                Verdict::Right | Verdict::Unproven => {
                    self.opened = Some(index);
                    return self.open_all(index, out);
                }
                Verdict::Wrong => self.candidates[index].folder_wrong = true,
                Verdict::Failed => return self.give_up(Emit::NeedsEngine(Why::Unsupported), out),
            }
        }
        self.ask(out);
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
                .derive_key(password, &ArchiveLimits::default())
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

    fn prove(&self, index: usize) -> Verdict {
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
        if crc32(&plain[from..from + entry.length as usize]) == expected {
            return Verdict::Right;
        }
        let params = [properties.cycles_power()];
        self.keys
            .mark_wrong(&self.folder_tuple(index, &properties, &params));
        Verdict::Wrong
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
                Some((cbc, padding)) => self.folders[at] = Lock::Open(Box::new(cbc), padding),
                None => failed = true,
            }
        }
        self.map = Some(map);
        if failed {
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        }
        // The proof's bytes were kept, not held: they go to their members
        // here, and the owner holds the rest of the folder's.
        if let Some(proof) = self.proof.take() {
            for (range, bytes, provenance) in self.kept.take(proof.range) {
                self.route(range, &bytes, provenance, out);
            }
        }
    }
}
