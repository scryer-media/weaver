// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Passwords, through unrar-rs and the set's one KDF cache. Enciphered
// headers open only on a candidate the archive's own check verifies: a
// header CRC is no proof, and RAR4 headers carry no check, so they go to an
// engine. An AES file opens on the first candidate its check does not
// refute; a file with no check, every RAR4 file among them, opens on the
// first candidate, and its CRC at the close is the backstop.

use unrar_rs::{
    KdfCache, MemberKeying, PasswordCheck, RarVolumeHeaderEncryption, check_member_password,
};
use weaver_graph::{Emit, Why};
use zeroize::Zeroizing;

use super::{Lock, RarStore};
use crate::crypt::Cbc;

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

// The derivation for one candidate, through the cache, so the check and the
// key derive the tuple once.
fn derive(cache: &KdfCache, password: &str, keying: &MemberKeying) -> Derived {
    match keying {
        MemberKeying::Rar5(facts) => {
            match check_member_password(
                cache,
                password,
                &facts.salt,
                facts.kdf_count_lg2,
                facts.psw_check.as_ref(),
            ) {
                PasswordCheck::Wrong => return Derived::Wrong,
                PasswordCheck::Verified | PasswordCheck::Unverifiable => {}
            }
            let Ok(material) =
                cache.derive_material_rar5(password, &facts.salt, facts.kdf_count_lg2)
            else {
                return Derived::Unkeyable;
            };
            Derived::Key(FileKey {
                key: Zeroizing::new(material.key.to_vec()),
                iv: facts.iv,
                hash_key: Some(Zeroizing::new(material.hash_key)),
            })
        }
        MemberKeying::Rar4 { salt } => {
            let (key, iv) = cache.derive_key_rar4(password, salt.as_ref());
            Derived::Key(FileKey {
                key: Zeroizing::new(key.to_vec()),
                iv,
                hash_key: None,
            })
        }
    }
}

impl RarStore {
    // Ask the job for a password, once: an offer answers it, and asking
    // again would only ask the same question.
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
        for index in 0..self.candidates.len() {
            if self.candidates[index].headers_wrong {
                continue;
            }
            match check_member_password(
                &self.kdf,
                &self.candidates[index].secret,
                &facts.salt,
                facts.kdf_count_lg2,
                Some(check),
            ) {
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
                PasswordCheck::Wrong => self.candidates[index].headers_wrong = true,
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
            let (Lock::Locked, Some(store), Some(length)) =
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
                match derive(&self.kdf, &self.candidates[*candidate].secret, &keying) {
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
            let Ok(cbc) = Cbc::new(&key.key, key.iv, cipher_len, length) else {
                return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            };
            let entry = &mut self.entries[index];
            entry.lock = Lock::Open(Box::new(cbc));
            entry.hash_key = key.hash_key;
            self.stale = true;
        }
        if asks {
            self.ask(out);
        }
    }

    // Everything that could arrive has: a password that has not opened the
    // set by now never will.
    pub(super) fn exhausted(&mut self, out: &mut Vec<Emit>) {
        if !self.asked || !self.finished() {
            return;
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
            return self.give_up(Emit::Refuse(Why::EncryptedHeader), out);
        }
        if self
            .entries
            .iter()
            .any(|entry| matches!(entry.lock, Lock::Locked))
        {
            self.give_up(Emit::Refuse(Why::Encrypted), out);
        }
    }
}
