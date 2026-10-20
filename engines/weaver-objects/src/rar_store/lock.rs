// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Passwords, through unrar-rs and the process's one key cache. Enciphered
// headers open only on a candidate the archive's own check verifies: a
// header CRC is no proof, and RAR4 headers carry no check, so they go to an
// engine. An AES file opens on the first candidate its check does not
// refute; a file with no check, every RAR4 file among them, opens on the
// first candidate, and its CRC at the close is the backstop.

use std::sync::Arc;

use unrar_rs::{MemberKeying, PasswordCheck, RarVolumeHeaderEncryption, check_member_password};
use weaver_graph::{Emit, Why};
use zeroize::Zeroizing;

use super::{Lock, RarStore};
use crate::crypt::Cbc;
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

    // The password the archive's check proved, for every header walk.
    pub(super) fn header_password(&self) -> Option<Zeroizing<String>> {
        self.headers_open
            .map(|index| self.candidates[index].secret.clone())
    }

    // The header-encryption decision, made before any header is deciphered.
    pub(super) fn open_headers(
        &mut self,
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
            RarVolumeHeaderEncryption::Rar4 | RarVolumeHeaderEncryption::None => {
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
                PasswordCheck::Verified => {
                    self.headers_open = Some(index);
                    for volume in self.volumes.values_mut() {
                        volume.walk.touch();
                    }
                    self.stale = true;
                    return;
                }
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
                match derive(&self.keys, &self.candidates[*candidate].secret, &keying) {
                    Derived::Key(key) => {
                        opened = Some(key);
                        break;
                    }
                    Derived::Wrong => {}
                    Derived::Unkeyable => {
                        return self.give_up(Emit::NeedsEngine(Why::Encrypted), out);
                    }
                }
            }
            let Some(key) = opened else {
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
            self.stale = true;
        }
        if asks {
            self.ask(out);
        }
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
