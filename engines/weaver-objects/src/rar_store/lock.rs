// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Passwords. Enciphered headers are opened by the candidate RAR5's check
// value proves, or, without one, by the first header's CRC after
// deciphering it, which is RAR4's only proof. An AES file is opened by the
// candidate its own check value proves; a file with none, every RAR4 file
// among them, is opened by the first candidate, unproven, and its CRC at the
// close is the backstop. Every key goes through the process's cache, so a
// tuple is derived once and a wrong one is tried once.

use std::sync::Arc;

use weaver_graph::{Emit, Why};
use zeroize::Zeroizing;

use super::header::FileCrypt;
use super::kdf;
use super::walk::{Cipher, HeaderKey, KeyFor, Step};
use super::{Lock, RarStore};
use crate::crypt::{BLOCK, Cbc};
use crate::keys::{Derived, Keys, Tuple};

const RAR5_TAG: &str = "rar5";
const RAR4_TAG: &str = "rar4";

enum Derive {
    Key(Zeroizing<Vec<u8>>),
    Wrong,
    // A derivation the store cannot run: an exponent past RAR's bound, or a
    // RAR4 password long enough to need RAR's own SHA-1.
    Unsupported,
}

enum Fail {
    Wrong,
    Unsupported,
}

// The AES-256 key and the hash key, as one cached entry. A tuple with a
// check value is cached only once the check passed.
fn rar5_key(
    keys: &Keys,
    secret: &str,
    salt: &[u8; 16],
    count: u8,
    check: Option<[u8; 8]>,
) -> Derive {
    let mut params = vec![count];
    params.extend(check.iter().flatten());
    let tuple = Tuple {
        format: RAR5_TAG,
        password: secret,
        salt,
        params: &params,
    };
    let derived = keys.derive(&tuple, || {
        let derived = kdf::rar5(secret, salt, count).ok_or(Fail::Unsupported)?;
        if check.is_some_and(|check| check != derived.check) {
            return Err(Fail::Wrong);
        }
        let mut both = Zeroizing::new(Vec::with_capacity(64));
        both.extend_from_slice(derived.key.as_slice());
        both.extend_from_slice(derived.hash_key.as_slice());
        Ok(both)
    });
    match derived {
        Ok(Derived::Key(key)) => Derive::Key(key),
        Ok(Derived::Wrong) => Derive::Wrong,
        Err(Fail::Wrong) => {
            keys.mark_wrong(&tuple);
            Derive::Wrong
        }
        Err(Fail::Unsupported) => Derive::Unsupported,
    }
}

// The AES-128 key and the IV, as one cached entry.
fn rar4_key(keys: &Keys, secret: &str, salt: &[u8; 8]) -> Derive {
    let tuple = Tuple {
        format: RAR4_TAG,
        password: secret,
        salt,
        params: &[],
    };
    let derived = keys.derive(&tuple, || {
        let derived = kdf::rar4(secret, salt).ok_or(())?;
        let mut both = Zeroizing::new(Vec::with_capacity(32));
        both.extend_from_slice(derived.key.as_slice());
        both.extend_from_slice(&derived.iv);
        Ok(both)
    });
    match derived {
        Ok(Derived::Key(key)) => Derive::Key(key),
        Ok(Derived::Wrong) => Derive::Wrong,
        Err(()) => Derive::Unsupported,
    }
}

pub(super) type Keyring = Box<dyn Fn(KeyFor<'_>) -> Option<HeaderKey>>;

fn keyring(keys: Arc<Keys>, secret: Zeroizing<String>) -> Keyring {
    Box::new(move |what| match what {
        KeyFor::Rar5(crypt) => {
            match rar5_key(&keys, &secret, &crypt.salt, crypt.count, crypt.check) {
                Derive::Key(both) => Some(HeaderKey {
                    key: Zeroizing::new(both[..32].to_vec()),
                    iv: None,
                }),
                Derive::Wrong | Derive::Unsupported => None,
            }
        }
        KeyFor::Rar4(salt) => match rar4_key(&keys, &secret, salt) {
            Derive::Key(both) => {
                let mut iv = [0; 16];
                iv.copy_from_slice(&both[16..32]);
                Some(HeaderKey {
                    key: Zeroizing::new(both[..16].to_vec()),
                    iv: Some(iv),
                })
            }
            Derive::Wrong | Derive::Unsupported => None,
        },
    })
}

// A file's key, IV and hash key.
struct FileKey {
    key: Zeroizing<Vec<u8>>,
    iv: [u8; 16],
    hash_key: Option<Zeroizing<[u8; 32]>>,
}

enum Opened {
    Key(FileKey),
    Wrong,
    Unsupported,
}

fn file_key(keys: &Keys, secret: &str, crypt: &FileCrypt) -> Opened {
    match crypt {
        FileCrypt::Rar5 {
            count,
            salt,
            iv,
            check,
            ..
        } => match rar5_key(keys, secret, salt, *count, *check) {
            Derive::Key(both) => {
                let mut hash_key = Zeroizing::new([0u8; 32]);
                hash_key.copy_from_slice(&both[32..64]);
                Opened::Key(FileKey {
                    key: Zeroizing::new(both[..32].to_vec()),
                    iv: *iv,
                    hash_key: Some(hash_key),
                })
            }
            Derive::Wrong => Opened::Wrong,
            Derive::Unsupported => Opened::Unsupported,
        },
        FileCrypt::Rar4 { salt } => match rar4_key(keys, secret, salt) {
            Derive::Key(both) => {
                let mut iv = [0; 16];
                iv.copy_from_slice(&both[16..32]);
                Opened::Key(FileKey {
                    key: Zeroizing::new(both[..16].to_vec()),
                    iv,
                    hash_key: None,
                })
            }
            Derive::Wrong => Opened::Wrong,
            Derive::Unsupported => Opened::Unsupported,
        },
        FileCrypt::Other => Opened::Unsupported,
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

    pub(super) fn header_keyring(&self) -> Option<Keyring> {
        let index = self.headers_open?;
        Some(keyring(
            self.keys.clone(),
            self.candidates[index].secret.clone(),
        ))
    }

    // A volume whose next header is enciphered and not yet opened.
    fn locked_headers(&self) -> Option<super::VolumeIndex> {
        self.volumes
            .iter()
            .find(|(_, volume)| volume.walk.cipher.is_some() && !volume.walk.ended)
            .map(|(index, _)| *index)
    }

    pub(super) fn open_headers(&mut self, out: &mut Vec<Emit>) {
        if self.headers_open.is_some() {
            return;
        }
        let Some(volume) = self.locked_headers() else {
            return;
        };
        for index in 0..self.candidates.len() {
            if self.candidates[index].headers_wrong {
                continue;
            }
            let secret = self.candidates[index].secret.clone();
            let found = self.volumes.get_mut(&volume).expect("volume is known");
            if let Some(Cipher::Rar5(crypt)) = found.walk.cipher
                && crypt.check.is_some()
            {
                match rar5_key(&self.keys, &secret, &crypt.salt, crypt.count, crypt.check) {
                    Derive::Key(_) => {
                        self.headers_open = Some(index);
                        self.stale = true;
                        return;
                    }
                    Derive::Wrong => {
                        self.candidates[index].headers_wrong = true;
                        continue;
                    }
                    Derive::Unsupported => {
                        return self.give_up(Emit::NeedsEngine(Why::Encrypted), out);
                    }
                }
            }
            let ring = keyring(self.keys.clone(), secret);
            let mut trial = found.walk.clone();
            match trial.step(&mut found.kept, found.length, Some(&*ring)) {
                Step::Moved => {
                    self.headers_open = Some(index);
                    self.stale = true;
                    return;
                }
                Step::Wait => return,
                Step::Damaged(range, _) if !found.kept.verified(range) => return,
                _ => self.candidates[index].headers_wrong = true,
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
            let (Lock::Locked, Some(crypt), Some(length)) =
                (&entry.lock, &entry.crypt, entry.length)
            else {
                continue;
            };
            let mut opened = None;
            for candidate in &order {
                match file_key(&self.keys, &self.candidates[*candidate].secret, crypt) {
                    Opened::Key(key) => {
                        opened = Some(key);
                        break;
                    }
                    Opened::Wrong => {}
                    Opened::Unsupported => {
                        return self.give_up(Emit::NeedsEngine(Why::Encrypted), out);
                    }
                }
            }
            let Some(key) = opened else {
                asks = true;
                continue;
            };
            let cipher_len = length.div_ceil(BLOCK) * BLOCK;
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
            && self.locked_headers().is_some()
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
