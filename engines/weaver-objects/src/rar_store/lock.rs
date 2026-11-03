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

use unrar_rs::{
    MemberKeying, PasswordCheck, RarVolumeHeaderEncryption, check_member_password,
    decrypt_cipher_range_rar4, rar4_derive_key,
};
use weaver_graph::{ByteRange, Emit, VolumeIndex, Why};
use zeroize::Zeroizing;

use super::{Candidate, Condemned, Lock, RarStore};
use crate::crypt::Cbc;
use crate::grid::crc32;
use crate::kept::Kept;
use crate::keys::{self, Keys, Tuple};

// A file's key, IV and hash key.
struct FileKey {
    key: Zeroizing<Vec<u8>>,
    iv: [u8; 16],
    hash_key: Option<Zeroizing<[u8; 32]>>,
}

enum Derived {
    Key(FileKey),
    Wrong,
    // A KDF count the crate refuses: nothing can decipher the member.
    Unkeyable,
}

// The derivation for one candidate, once per tuple in the process: the key
// cache holds the key, or the finding that the check refutes the password.
// A RAR5 entry is the key then the hash key; a RAR4 one the key then the IV.
fn derive(keys: &Keys, password: &str, keying: &MemberKeying) -> Derived {
    let kdf = keys.kdf();
    let (format, salt, count): (_, &[u8], _) = match keying {
        MemberKeying::Rar5(facts) => ("rar5", &facts.salt, [facts.kdf_count_lg2]),
        MemberKeying::Rar4 { salt } => ("rar4", salt.as_ref().map_or(&[], |salt| salt), [0]),
    };
    let tuple = Tuple {
        format,
        password,
        salt,
        params: &count,
    };
    let derived = keys.derive(&tuple, || match keying {
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
            keys.mark_wrong(&tuple);
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
            password,
        )
    }

    // The candidates in the order to try them: the one that opened the
    // headers first.
    fn order(&self) -> Vec<usize> {
        let mut order: Vec<usize> = self.headers_open.into_iter().collect();
        order.extend((0..self.candidates.len()).filter(|index| Some(*index) != self.headers_open));
        order
    }

    pub(super) fn open_files(&mut self, out: &mut Vec<Emit>) {
        let order = self.order();
        let mut asks = false;
        for index in 0..self.entries.len() {
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
            for candidate in &order {
                if self.candidates[*candidate].files_wrong {
                    continue;
                }
                match derive(&self.keys, &self.candidates[*candidate].secret, &keying) {
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
            let Some((key, trial)) = opened else {
                asks = true;
                continue;
            };
            let entry = &mut self.entries[index];
            // Sealed at a restart: the blocks it kept, keyed again.
            let opened = match std::mem::replace(&mut entry.lock, Lock::Locked) {
                Lock::Sealed(chain) if chain.fits(cipher_len, length) => {
                    Cbc::unseal(&key.key, *chain)
                }
                _ => Cbc::new(&key.key, key.iv, cipher_len, length),
            };
            let Ok(cbc) = opened else {
                return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            };
            entry.lock = Lock::Open(Arc::new(cbc));
            entry.hash_key = key.hash_key;
            entry.trial = trial;
            self.stale = true;
        }
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
        for (id, entry) in self.entries.iter_mut().enumerate() {
            if entry.trial != Some(candidate) {
                continue;
            }
            let Lock::Open(cbc) = std::mem::replace(&mut entry.lock, Lock::Locked) else {
                continue;
            };
            let parts = entry.pieces.iter().map(|piece| piece.volume).collect();
            let said = std::mem::take(&mut entry.out);
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
        self.entries
            .iter()
            .any(|entry| matches!(entry.lock, Lock::Locked | Lock::Sealed(_)))
            .then_some(Why::Encrypted)
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
    password: &str,
) -> Option<bool> {
    let main = read(ByteRange::at(RAR4_SIGNATURE, RAR4_BASE))?;
    let at = RAR4_SIGNATURE + u64::from(u16::from_le_bytes([main[5], main[6]]));
    let front = read(ByteRange::at(at, RAR4_SALT + AES_BLOCK as u64))?;
    let salt: [u8; 8] = front[..8].try_into().ok()?;
    let mut block: [u8; AES_BLOCK] = front[8..].try_into().ok()?;
    let (key, iv) = rar4_derive_key(password, Some(&salt));
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

// The bytes of `range` when they are here whole; otherwise they are wanted.
fn whole(kept: &mut Kept, range: ByteRange) -> Option<&[u8]> {
    if !kept.verified(range) {
        kept.want(range);
        return None;
    }
    kept.read(range)
}
