// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Passwords, through unrar-rs and the process's one key cache. Enciphered
// headers open only on a candidate the archive's own check proves: RAR5
// checks its password value; RAR4 has none, so its first enciphered header
// is the check, as unrar takes it: a candidate proves itself when that
// header unseals under it with its CRC whole. An AES file opens on the first
// candidate its check does not refute; a file with no check, every RAR4
// file among them, opens on the first candidate, and its CRC at the close
// is the backstop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use unrar_rs::{
    MemberKeying, PasswordCheck, RarVolumeHeaderEncryption, check_member_password,
    decrypt_cipher_range_rar4,
};
use weaver_graph::{ByteRange, Emit, VolumeIndex, Why};
use zeroize::Zeroizing;

use super::{Candidate, Condemned, Lock, RarStore};
use crate::crypt::{Cbc, Padding};
use crate::grid::crc32;
use crate::kept::Kept;
use crate::keys::{self, Keys, Tuple};

// A file's key, IV and hash key.
pub(super) struct FileKey {
    key: Zeroizing<Vec<u8>>,
    iv: [u8; 16],
    hash_key: Option<Zeroizing<[u8; 32]>>,
}

pub(super) enum Derived {
    Key(FileKey),
    Wrong,
    // A KDF count the crate refuses: nothing can decipher the member.
    Unkeyable,
}

// The cache's tuple for a candidate's key to a file.
fn with_tuple<R>(password: &str, keying: &MemberKeying, then: impl FnOnce(&Tuple<'_>) -> R) -> R {
    let (format, salt, count): (_, &[u8], _) = match keying {
        MemberKeying::Rar5(facts) => ("rar5", &facts.salt, [facts.kdf_count_lg2]),
        MemberKeying::Rar4 { salt } => ("rar4", salt.as_ref().map_or(&[], |salt| salt), [0]),
    };
    then(&Tuple {
        format,
        password,
        salt,
        params: &count,
    })
}

// The answer the cache holds for a candidate's key to a file: the key, or
// that the check refutes the password. Without one the derivation is too
// slow for the loop. One lookup, acted on as it answered: the cache is
// shared, and may drop the entry before a second.
fn cached(keys: &Keys, password: &str, keying: &MemberKeying) -> Option<Derived> {
    with_tuple(password, keying, |tuple| match keys.get(tuple)? {
        keys::Derived::Key(material) => Some(Derived::Key(file_key(&material, keying))),
        keys::Derived::Wrong => Some(Derived::Wrong),
    })
}

// The derivation for one candidate, once per tuple in the process: the key
// cache holds the key, or the finding that the check refutes the password.
// A RAR5 entry is the key then the hash key; a RAR4 one the key then the IV.
fn derive(keys: &Keys, password: &str, keying: &MemberKeying) -> Derived {
    with_tuple(password, keying, |tuple| {
        derive_tuple(keys, password, keying, tuple)
    })
}

fn derive_tuple(keys: &Keys, password: &str, keying: &MemberKeying, tuple: &Tuple<'_>) -> Derived {
    let kdf = keys.kdf();
    let count = match keying {
        MemberKeying::Rar5(facts) => [facts.kdf_count_lg2],
        MemberKeying::Rar4 { .. } => [0],
    };
    let derived = keys.derive(tuple, || match keying {
        MemberKeying::Rar5(facts) => {
            let check = facts.psw_check.as_ref();
            match check_member_password(kdf, password, &facts.salt, count[0], check) {
                PasswordCheck::Wrong => return Err(Derived::Wrong),
                PasswordCheck::Verified | PasswordCheck::Unverifiable => {}
            }
            let material = kdf
                .derive_material_rar5(password, &facts.salt, count[0])
                .map_err(|_| Derived::Unkeyable)?;
            Ok(Zeroizing::new([material.key, material.hash_key].concat()))
        }
        MemberKeying::Rar4 { salt } => {
            let (key, iv) = kdf.derive_key_rar4(password, salt.as_ref());
            Ok(Zeroizing::new([&key[..], &iv[..]].concat()))
        }
    });
    match derived {
        Ok(keys::Derived::Key(material)) => Derived::Key(file_key(&material, keying)),
        Ok(keys::Derived::Wrong) => Derived::Wrong,
        Err(Derived::Wrong) => {
            keys.mark_wrong(tuple);
            Derived::Wrong
        }
        Err(other) => other,
    }
}

// A file's key out of a cache entry.
fn file_key(material: &[u8], keying: &MemberKeying) -> FileKey {
    let mut half = [0; 32];
    match keying {
        MemberKeying::Rar5(facts) => {
            half.copy_from_slice(&material[32..64]);
            let key = Zeroizing::new(material[..32].to_vec());
            FileKey {
                key,
                iv: facts.iv,
                hash_key: Some(Zeroizing::new(half)),
            }
        }
        MemberKeying::Rar4 { .. } => {
            let mut iv = [0; 16];
            iv.copy_from_slice(&material[16..32]);
            FileKey {
                key: Zeroizing::new(material[..16].to_vec()),
                iv,
                hash_key: None,
            }
        }
    }
}

impl RarStore {
    // Ask the job for a password, once until an offer answers it: asking
    // again before then would only ask the same question.
    pub(super) fn ask(&mut self, out: &mut Vec<Emit>) {
        if !self.asked {
            self.asked = true;
            out.push(Emit::NeedsPassword);
        }
    }

    // The candidate that is `secret`, added when the store has none.
    pub(super) fn candidate(&mut self, secret: &str) -> usize {
        let found = self
            .candidates
            .iter()
            .position(|candidate| candidate.secret.as_str() == secret);
        found.unwrap_or_else(|| {
            self.candidates.push(Candidate {
                secret: Zeroizing::new(secret.to_owned()),
                headers_wrong: false,
                files_wrong: false,
            });
            self.candidates.len() - 1
        })
    }

    // The password the archive's check proved, for every header walk.
    pub(super) fn header_password(&self) -> Option<Zeroizing<String>> {
        self.headers_open
            .map(|index| self.candidates[index].secret.clone())
    }

    // The header-encryption decision, made before any header is deciphered.
    // A check derives a key, so it is the store's work, done away from the
    // loop: here only what needs no key is decided, and the volume is due.
    pub(super) fn open_headers(
        &mut self,
        volume: VolumeIndex,
        encryption: &RarVolumeHeaderEncryption,
        out: &mut Vec<Emit>,
    ) {
        // A proved password that will not open this volume: the volumes
        // disagree about their archive key.
        if self.headers_open.is_some() {
            return self.give_up(Emit::NeedsEngine(Why::EncryptedHeader), out);
        }
        match encryption {
            RarVolumeHeaderEncryption::Rar5(facts) if facts.psw_check.is_some() => {
                let left = (0..self.candidates.len()).any(|index| !self.rar5_refuted(index, facts));
                if !left {
                    return self.ask(out);
                }
            }
            RarVolumeHeaderEncryption::Rar4 => {
                if self
                    .candidates
                    .iter()
                    .all(|candidate| candidate.headers_wrong)
                {
                    return self.ask(out);
                }
                // Nothing is checked until the header's front is here whole:
                // a later walk asks again once it is.
                if !self.rar4_front_whole(volume) {
                    return;
                }
            }
            _ => return self.give_up(Emit::NeedsEngine(Why::EncryptedHeader), out),
        }
        self.headers_due.insert(volume, *encryption);
    }

    // Whether the cache, or a check before, already refutes the candidate at
    // `index` for these headers: no key is derived to know it.
    fn rar5_refuted(
        &mut self,
        index: usize,
        facts: &unrar_rs::RarVolumeHeaderEncryptionFacts,
    ) -> bool {
        let tuple = Tuple {
            format: "rar5",
            password: &self.candidates[index].secret,
            salt: &facts.salt,
            params: &[facts.kdf_count_lg2],
        };
        let refuted = self.candidates[index].headers_wrong || self.keys.is_wrong(&tuple);
        self.candidates[index].headers_wrong = refuted;
        refuted
    }

    // Check the headers of every volume due, away from the loop. Once
    // `stop` is set no further check is begun, and the rest stay due;
    // `false` then.
    pub(super) fn check_headers_due(&mut self, stop: &AtomicBool, out: &mut Vec<Emit>) -> bool {
        let mut due = std::mem::take(&mut self.headers_due).into_iter();
        while let Some((volume, encryption)) = due.next() {
            if stop.load(Ordering::Relaxed) {
                self.headers_due.insert(volume, encryption);
                self.headers_due.extend(due);
                return false;
            }
            // A check before this one proved a candidate, and every walk
            // reads under it now; or the volume went back.
            if self.gone || self.headers_open.is_some() || !self.volumes.contains_key(&volume) {
                continue;
            }
            self.check_headers(volume, &encryption, out);
        }
        true
    }

    // Each candidate in turn against the archive's own check, first one
    // proved opening the headers.
    fn check_headers(
        &mut self,
        volume: VolumeIndex,
        encryption: &RarVolumeHeaderEncryption,
        out: &mut Vec<Emit>,
    ) {
        let facts = match encryption {
            RarVolumeHeaderEncryption::Rar5(facts) => facts,
            RarVolumeHeaderEncryption::Rar4 => return self.open_rar4_headers(volume, out),
            RarVolumeHeaderEncryption::None => {
                return self.give_up(Emit::NeedsEngine(Why::EncryptedHeader), out);
            }
        };
        let Some(check) = facts.psw_check.as_ref() else {
            return self.give_up(Emit::NeedsEngine(Why::EncryptedHeader), out);
        };
        let (format, salt, count) = ("rar5", &facts.salt[..], [facts.kdf_count_lg2]);
        for index in 0..self.candidates.len() {
            let secret = self.candidates[index].secret.clone();
            let tuple = Tuple {
                format,
                password: &secret,
                salt,
                params: &count,
            };
            if self.candidates[index].headers_wrong || self.keys.is_wrong(&tuple) {
                self.candidates[index].headers_wrong = true;
                continue;
            }
            let kdf = self.keys.kdf();
            match check_member_password(kdf, &secret, &facts.salt, count[0], Some(check)) {
                PasswordCheck::Verified => return self.open_on(index),
                PasswordCheck::Unverifiable => {
                    return self.give_up(Emit::NeedsEngine(Why::EncryptedHeader), out);
                }
                PasswordCheck::Wrong => {
                    self.keys.mark_wrong(&tuple);
                    self.candidates[index].headers_wrong = true;
                }
            }
        }
        self.ask(out);
    }

    // Every walk reads the headers again under the candidate at `index`,
    // and every recovery record is said again, handed it: after a restore
    // too, when the record built before holds no candidate.
    fn open_on(&mut self, index: usize) {
        self.headers_open = Some(index);
        self.recovery_said.clear();
        self.dirty.extend(self.volumes.keys().copied());
        self.stale = true;
    }

    // Each candidate in turn against the first enciphered header of
    // `volume`. Until that header's bytes are here whole nothing is proved
    // or refuted; a later walk asks again once they are.
    fn open_rar4_headers(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        for index in 0..self.candidates.len() {
            if self.candidates[index].headers_wrong {
                continue;
            }
            let secret = self.candidates[index].secret.clone();
            match self.rar4_unseals(volume, &secret) {
                Some(true) => return self.open_on(index),
                Some(false) => self.candidates[index].headers_wrong = true,
                None => return,
            }
        }
        self.ask(out);
    }

    // Whether the first enciphered header of `volume` unseals under
    // `password`, read from what the volume kept; `None` while its bytes are
    // not here whole, which wants them.
    fn rar4_unseals(&mut self, volume: VolumeIndex, password: &str) -> Option<bool> {
        let found = self.volumes.get_mut(&volume)?;
        let length = found.length;
        let kept = &mut found.kept;
        rar4_unseals(
            &mut |range| whole(kept, range).map(<[u8]>::to_vec),
            length,
            self.keys.kdf(),
            password,
        )
    }

    // Whether the front of the first enciphered header of `volume` is here
    // whole: what a check reads before it derives a key. Not yet, it is
    // wanted.
    fn rar4_front_whole(&mut self, volume: VolumeIndex) -> bool {
        let Some(found) = self.volumes.get_mut(&volume) else {
            return false;
        };
        let kept = &mut found.kept;
        rar4_front(&mut |range| whole(kept, range).map(<[u8]>::to_vec)).is_some()
    }

    // The candidates in the order to try them: the one that opened the
    // headers first.
    fn order(&self) -> Vec<usize> {
        let mut order: Vec<usize> = self.headers_open.into_iter().collect();
        order.extend((0..self.candidates.len()).filter(|index| Some(*index) != self.headers_open));
        order
    }

    // Derive every key a file is waiting on, away from the loop, into the
    // cache: the files open from it once the store is back. Once `stop` is
    // set no further key is begun, and every file with a key still to
    // derive stays due; `false` then.
    pub(super) fn derive_due(&mut self, stop: &AtomicBool) -> bool {
        let mut due = std::mem::take(&mut self.derive_due).into_iter();
        while let Some((candidate, index)) = due.next() {
            if stop.load(Ordering::Relaxed) {
                self.derive_due.insert((candidate, index));
                self.derive_due.extend(due);
                return false;
            }
            let (Some(found), Some(store)) = (
                self.candidates.get(candidate),
                self.entries
                    .get(index)
                    .and_then(|entry| entry.store.as_ref()),
            ) else {
                continue;
            };
            let derived = derive(&self.keys, &found.secret, &store.keying());
            self.derived.insert((candidate, index), derived);
        }
        self.stale = true;
        true
    }

    pub(super) fn open_files(&mut self, out: &mut Vec<Emit>) {
        let order = self.order();
        let mut asks = false;
        let mut due = Vec::new();
        #[cfg(not(test))]
        let locked: Vec<usize> = self.locked.iter().copied().collect();
        #[cfg(test)]
        let locked: Vec<usize> = match super::counts::old() {
            true => (0..self.entries.len()).collect(),
            false => self.locked.iter().copied().collect(),
        };
        #[cfg(test)]
        super::counts::bump(|counts| counts.keyed += locked.len());
        #[cfg(test)]
        if !super::counts::old() {
            let all: Vec<usize> = (0..self.entries.len())
                .filter(|&index| matches!(self.entries[index].lock, Lock::Locked | Lock::Sealed(_)))
                .collect();
            assert_eq!(locked, all, "the locked entries are filed");
        }
        for index in locked {
            let entry = &self.entries[index];
            let (Lock::Locked | Lock::Sealed(_), Some(store), Some(length)) =
                (&entry.lock, &entry.store, entry.length)
            else {
                continue;
            };
            let Some(cipher_len) = store.cipher_size else {
                continue;
            };
            let keying = store.keying();
            let mut opened = None;
            let mut waits = false;
            for candidate in &order {
                if self.candidates[*candidate].files_wrong {
                    continue;
                }
                let secret = &self.candidates[*candidate].secret;
                // A key the cache has no answer for is derived on the pool:
                // the file waits for it, and no later candidate is tried.
                let derived = match self
                    .derived
                    .remove(&(*candidate, index))
                    .or_else(|| cached(&self.keys, secret, &keying))
                {
                    Some(derived) => derived,
                    None => {
                        due.push((*candidate, index));
                        waits = true;
                        break;
                    }
                };
                match derived {
                    Derived::Key(key) => {
                        // A RAR4 file key has no check of its own: unless
                        // the candidate proved itself on the headers, it is
                        // on trial.
                        let unproven = matches!(keying, MemberKeying::Rar4 { .. })
                            && self.headers_open != Some(*candidate);
                        opened = Some((key, unproven.then_some(*candidate)));
                        break;
                    }
                    Derived::Wrong => {}
                    Derived::Unkeyable => {
                        return self.give_up(Emit::NeedsEngine(Why::Encrypted), out);
                    }
                }
            }
            if waits {
                continue;
            }
            let Some((key, trial)) = opened else {
                asks = true;
                continue;
            };
            let entry = &mut self.entries[index];
            // Sealed at a restart: the blocks it kept, keyed again, when
            // they and the padding kept beside them fit the stream. A kept
            // padding belongs to the chain kept beside it: a chain not used
            // takes its padding with it, and a padding that does not fit
            // goes whatever the lock, so a fresh cipher starts with none.
            let fits = entry.out.padding.fits(cipher_len.saturating_sub(length));
            let (opened, forget) = match std::mem::replace(&mut entry.lock, Lock::Locked) {
                Lock::Sealed(chain) if fits && chain.fits(cipher_len, length) => {
                    (Cbc::unseal(&key.key, *chain), false)
                }
                Lock::Sealed(_) => (Cbc::new(&key.key, key.iv, cipher_len, length), true),
                _ => (Cbc::new(&key.key, key.iv, cipher_len, length), !fits),
            };
            if forget {
                Arc::make_mut(&mut entry.out).padding = Padding::default();
            }
            let Ok(cbc) = opened else {
                return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            };
            entry.lock = Lock::Open(Arc::new(cbc));
            self.locked.remove(&index);
            entry.hash_key = key.hash_key;
            entry.trial = trial;
            self.stale = true;
        }
        self.derive_due.extend(due);
        if asks {
            self.ask(out);
        }
    }

    // The candidate on trial is wrong: the next one is tried on the same
    // bytes, which every file it keyed asks for again. Each such file is
    // locked again at once, a fresh record beside it, and its parts read
    // back through the cipher and record it stood under until each one has
    // been posted again.
    pub(super) fn condemn(&mut self, candidate: usize, out: &mut Vec<Emit>) {
        self.candidates[candidate].files_wrong = true;
        self.moved();
        for (id, entry) in self.entries.iter_mut().enumerate() {
            if entry.trial != Some(candidate) {
                continue;
            }
            self.locked.insert(id);
            let Lock::Open(cbc) = std::mem::replace(&mut entry.lock, Lock::Locked) else {
                continue;
            };
            let parts = entry.pieces.iter().map(|piece| piece.volume).collect();
            let said = std::mem::take(&mut entry.out);
            entry.condemned_on = said.coverage.clone();
            self.condemned.insert(
                id,
                Condemned {
                    cbc,
                    out: said,
                    parts,
                },
            );
            entry.trial = None;
            entry.hash_key = None;
            entry.plain_runs = Default::default();
            out.extend(entry.pieces.iter().map(|piece| Emit::Again {
                volume: piece.volume,
                range: piece.data,
            }));
        }
        self.stale = true;
    }

    // A repair rewrote bytes a candidate was condemned over as they were
    // posted: a key on trial cannot tell a wrong key from wrong bytes, so
    // the checksum may have failed on the bytes. Every candidate is tried
    // again on the bytes as they now stand, and every locked file reads its
    // parts again under the one that keys it. A wrong one is condemned again
    // by the same checksum, over bytes the repair left as they now stand.
    pub(super) fn acquit(&mut self, out: &mut Vec<Emit>) {
        for candidate in &mut self.candidates {
            candidate.files_wrong = false;
        }
        for entry in &self.entries {
            if matches!(entry.lock, Lock::Locked) {
                out.extend(entry.pieces.iter().map(|piece| Emit::Again {
                    volume: piece.volume,
                    range: piece.data,
                }));
            }
        }
        self.stale = true;
    }

    // What the set is shut by while the store is still asking: its headers,
    // when no candidate opened them, or a file no candidate keys.
    pub(super) fn shut(&self) -> Option<Why> {
        if !self.asked {
            return None;
        }
        let headers_shut = self.headers_open.is_none()
            && self
                .volumes
                .values()
                .any(|volume| volume.walk.locked.is_some())
            && self
                .candidates
                .iter()
                .all(|candidate| candidate.headers_wrong);
        if headers_shut {
            return Some(Why::EncryptedHeader);
        }
        (!self.locked.is_empty()).then_some(Why::Encrypted)
    }
}

// A RAR4 volume's signature, the common part of every header, and the salt
// before each enciphered one.
const RAR4_SIGNATURE: u64 = 7;
const RAR4_BASE: u64 = 7;
const RAR4_SALT: u64 = 8;
const AES_BLOCK: usize = 16;

// Whether the first enciphered header of a RAR4 volume `length` long
// unseals under `password` with its CRC whole: the one proof a RAR4
// candidate has, as unrar takes it. Each RAR4 header is its salt, then the
// header ciphered to a whole block; under a wrong key the header is noise,
// so whichever of its size or CRC fails first refutes it. `read` gives the
// volume's bytes over a range when they are here whole; `None` while one
// the proof needs is not.
fn rar4_unseals(
    read: &mut dyn FnMut(ByteRange) -> Option<Vec<u8>>,
    length: Option<u64>,
    kdf: &unrar_rs::KdfCache,
    password: &str,
) -> Option<bool> {
    let (at, salt, mut block) = rar4_front(read)?;
    let (key, iv) = kdf.derive_key_rar4(password, Some(&salt));
    decrypt_cipher_range_rar4(&key, &iv, &mut block).ok()?;
    let size = usize::from(u16::from_le_bytes([block[5], block[6]]));
    let sealed = ByteRange::at(at + RAR4_SALT, size.next_multiple_of(AES_BLOCK) as u64);
    if (size as u64) < RAR4_BASE || length.is_some_and(|length| sealed.end > length) {
        return Some(false);
    }
    let mut header = read(sealed)?;
    decrypt_cipher_range_rar4(&key, &iv, &mut header).ok()?;
    let crc = crc32(&header[2..size]) & 0xffff;
    Some(crc == u32::from(u16::from_le_bytes([header[0], header[1]])))
}

// Where the first enciphered header of a RAR4 volume begins, its salt and
// its first block: all a check reads before it derives a key. `None` while
// the bytes are not here whole.
fn rar4_front(
    read: &mut dyn FnMut(ByteRange) -> Option<Vec<u8>>,
) -> Option<(u64, [u8; 8], [u8; AES_BLOCK])> {
    let main = read(ByteRange::at(RAR4_SIGNATURE, RAR4_BASE))?;
    let at = RAR4_SIGNATURE + u64::from(u16::from_le_bytes([main[5], main[6]]));
    let front = read(ByteRange::at(at, RAR4_SALT + AES_BLOCK as u64))?;
    let salt: [u8; 8] = front[..8].try_into().ok()?;
    let block: [u8; AES_BLOCK] = front[8..].try_into().ok()?;
    Some((at, salt, block))
}

// The bytes of `range` when they are here whole; otherwise they are wanted.
fn whole(kept: &mut Kept, range: ByteRange) -> Option<&[u8]> {
    if !kept.verified(range) {
        kept.want(range);
        return None;
    }
    kept.read(range)
}
