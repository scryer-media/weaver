//! Encrypted direct-store — plan 136, E-D1/E-D2/E-D3.
//!
//! An encrypted `Store` member's packed bytes are its plaintext as one
//! AES-256-CBC stream, so cipher offset and member-logical offset are the same
//! number and **every range answer plan 135 computes is unchanged**. What
//! changes is that the bytes on the wire are not the bytes that belong in the
//! destination: they pass through one transform on the way in.
//!
//! Three things live here, and nothing else does — the router owns the
//! coverage, the holds and the gates exactly as it did before.
//!
//! - **Admission** ([`KeyRing`]). One password per set, one key derivation per
//!   KDF tuple, and a verdict *before any byte routes*. The password is held in
//!   memory for the life of the set and is never written anywhere.
//! - **The write transform** ([`MemberCrypt`]). AES-CBC decrypts block *N* from
//!   block *N* and block *N−1* alone, so a router holding cipher bytes out of
//!   order can decrypt each span the moment its predecessor block has landed.
//!   The state that makes that work is two small maps: the 16 cipher bytes
//!   *ending* at a covered run's frontier ([`MemberCrypt::checkpoints`]) and the
//!   plaintext of a block only part of which has been emitted
//!   ([`MemberCrypt::edge_plain`]).
//! - **The keyed folds** (E-D3). A RAR5 writer may key a member's checksums with
//!   the KDF's hash key, which turns the whole-member CRC32 into the real
//!   wrong-password backstop: layer 1's per-part packed hashes cover *cipher*
//!   bytes and pass whatever the password was, so they detect a bad password not
//!   at all.
//!
//! # Why the padding is retained
//!
//! `cipher_size` is `align16(unpacked_size)`, so the final block's plaintext can
//! run up to 15 bytes past the member's end. Those bytes are never destination
//! bytes — but byte-exact re-encryption of the last block (E2, and every
//! posted-byte consumer with it) needs them, and they cannot be recovered from
//! the destination file because they are not in it. They are kept per member and
//! carried in the coverage snapshot.

use std::collections::BTreeMap;

use weaver_unrar::{
    EncryptedStore, KdfCache, PasswordCheck, RarVolumeMemberEncryptionFacts, check_member_password,
    convert_crc32_to_mac, decrypt_cipher_range, derive_rar5_material,
};

use super::CrcRuns;
use crate::pipeline::direct_store::ByteRanges;

/// AES block size, in the one place this module states it.
pub(crate) const AES_BLOCK: u64 = 16;

/// The block-aligned offset at or below `offset`.
pub(crate) fn block_floor(offset: u64) -> u64 {
    offset & !(AES_BLOCK - 1)
}

/// The block-aligned offset at or above `offset`. Saturates, which only a
/// hostile header can reach and which the layout has already refused by then
/// (`cipher_size` is `None` when `align16` overflows).
pub(crate) fn block_ceil(offset: u64) -> u64 {
    block_floor(offset.saturating_add(AES_BLOCK - 1))
}

/// Why an encrypted set may not route. Every variant is its own metric bucket
/// and every one of them demotes to the conventional path, which is the same
/// path the set would have taken before this plan existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CryptRefusal {
    /// No password reached the set. The conventional extractor will ask the
    /// job's candidate list — which is a superset of what direct-store sees —
    /// so demoting costs the direct route and nothing else.
    NoPassword,
    /// The header carried a password-check value and this password does not
    /// reproduce it. Nothing is written on the strength of a refuted password:
    /// the set demotes and the conventional path fails the same way, which is
    /// the parity this refusal exists to keep.
    WrongPassword,
    /// The headers state key material this build cannot derive from — a RAR4
    /// member (file encryption for RAR4/RAR3 is phase E3), or a KDF count the
    /// crate refuses.
    Unkeyable,
}

impl CryptRefusal {
    pub(crate) fn metric(self) -> &'static str {
        match self {
            Self::NoPassword => "encrypted_no_password",
            Self::WrongPassword => "encrypted_wrong_password",
            Self::Unkeyable => "encrypted_unkeyable",
        }
    }
}

/// One member's derived key material. Copied out of `weaver-unrar`'s zeroizing
/// carrier deliberately: the router needs the key for the life of the set, and
/// the alternative is re-running a 2^n PBKDF2 per span.
#[derive(Clone, Copy)]
pub(crate) struct MemberKeys {
    pub(crate) key: [u8; 32],
    pub(crate) hash_key: [u8; 32],
}

impl std::fmt::Debug for MemberKeys {
    /// Never prints key bytes. A key in a log is a key on disk.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MemberKeys(<redacted>)")
    }
}

/// The set's password and the keys derived from it, one derivation per KDF
/// tuple (E-D1).
///
/// # The password is never persisted
///
/// It lives here, in memory, for the life of the set. Nothing writes it to the
/// coverage snapshot, to the volume-facts cache or to a log; a restart with no
/// password demotes the set by name rather than trying to carry one across.
pub(crate) struct KeyRing {
    password: Option<String>,
    /// `weaver-unrar`'s own KDF cache. Two members of one set nearly always
    /// share a tuple, and a set with 200 members would otherwise pay 200
    /// PBKDF2 runs at admission.
    cache: KdfCache,
    /// Keys by `(salt, lg2 iteration count)` — the crypt tuple.
    keys: BTreeMap<([u8; 16], u8), MemberKeys>,
    /// Sticky: once a password has been refuted, re-deriving cannot un-refute
    /// it, and a later parse must reach the same answer.
    refusal: Option<CryptRefusal>,
    /// Whether any encrypted member has been admitted. Read by the eligibility
    /// gate, which counts an encrypted member routable only with one.
    admitted: bool,
}

impl std::fmt::Debug for KeyRing {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeyRing")
            .field("has_password", &self.password.is_some())
            .field("tuples", &self.keys.len())
            .field("refusal", &self.refusal)
            .field("admitted", &self.admitted)
            .finish()
    }
}

impl Default for KeyRing {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyRing {
    pub(crate) fn new() -> Self {
        Self {
            password: None,
            cache: KdfCache::new(),
            keys: BTreeMap::new(),
            refusal: None,
            admitted: false,
        }
    }

    /// Whether the set is still willing to take a password.
    ///
    /// A password can arrive **after** the job was added — `setJobPassword` and
    /// the NZBGet facade's `*Unpack:Password` both mutate the live spec — so the
    /// seam re-reads it while this is true. It stops the moment a password is
    /// held, so a set with one costs nothing.
    pub(crate) fn wants_password(&self) -> bool {
        self.password.is_none() && self.refusal.is_none()
    }

    /// Binds the job's password. A no-op once the same one is held, and refused
    /// outright once a password has been refuted: re-admitting on a *changed*
    /// password would need every routed byte re-decrypted, which is a demotion
    /// with extra steps.
    pub(crate) fn set_password(&mut self, password: Option<&str>) {
        if self.refusal.is_some() || self.admitted {
            return;
        }
        match password {
            Some(password)
                if !password.is_empty() && self.password.as_deref() != Some(password) =>
            {
                self.password = Some(password.to_string());
                self.keys.clear();
            }
            _ => {}
        }
    }

    /// Whether an encrypted member of this set has been admitted, which is what
    /// makes one *routable* rather than merely mapped.
    pub(crate) fn admitted(&self) -> bool {
        self.admitted
    }

    pub(crate) fn refusal(&self) -> Option<CryptRefusal> {
        self.refusal
    }

    /// The E-D1 decision for one encrypted member, made **before any byte of it
    /// routes**.
    ///
    /// - No password: refuse. An encrypted set routes only with one.
    /// - [`PasswordCheck::Wrong`]: refuse. The header states a value this
    ///   password does not reproduce, so every byte it decrypted would be
    ///   garbage.
    /// - [`PasswordCheck::Unverifiable`]: admit **provisionally**. The writer
    ///   omitted the check (or it failed its own tag), so nothing can be
    ///   concluded here and the member's keyed checksum gate is the earliest
    ///   detector — the same detection latency layer 1 has for a plaintext
    ///   member.
    /// - [`PasswordCheck::Verified`]: admit. Note that this is *not* a
    ///   guarantee: the check value is 8 unauthenticated bytes a hostile writer
    ///   chooses, and forging them admits a wrong password. The keyed member
    ///   gate is the authority either way, which is why this is an admission
    ///   test and never a reason to skip that gate.
    pub(crate) fn admit(
        &mut self,
        facts: &RarVolumeMemberEncryptionFacts,
    ) -> Result<MemberKeys, CryptRefusal> {
        if let Some(refusal) = self.refusal {
            return Err(refusal);
        }
        let Some(password) = self.password.clone() else {
            return Err(self.refuse(CryptRefusal::NoPassword));
        };
        let tuple = (facts.salt, facts.kdf_count_lg2);
        if let Some(keys) = self.keys.get(&tuple) {
            self.admitted = true;
            return Ok(*keys);
        }
        match check_member_password(
            &self.cache,
            &password,
            &facts.salt,
            facts.kdf_count_lg2,
            facts.psw_check.as_ref(),
        ) {
            PasswordCheck::Wrong => return Err(self.refuse(CryptRefusal::WrongPassword)),
            PasswordCheck::Verified | PasswordCheck::Unverifiable => {}
        }
        let Ok(mut material) = derive_rar5_material(&password, &facts.salt, facts.kdf_count_lg2)
        else {
            // A KDF count the crate refuses. `check_member_password` reports the
            // same tuple as `Unverifiable` rather than `Wrong`, so this is where
            // it is caught: nothing can decrypt the member at all.
            return Err(self.refuse(CryptRefusal::Unkeyable));
        };
        let keys = MemberKeys {
            key: material.key,
            hash_key: material.hash_key,
        };
        // The carrier zeroizes on drop; the copies above are the router's.
        material.key.fill(0);
        material.hash_key.fill(0);
        self.keys.insert(tuple, keys);
        self.admitted = true;
        Ok(keys)
    }

    fn refuse(&mut self, refusal: CryptRefusal) -> CryptRefusal {
        self.refusal.get_or_insert(refusal);
        self.keys.clear();
        refusal
    }
}

/// The crypt facts a restore needs to rebuild a member's keys without
/// re-parsing a header, plus the state that cannot be re-derived from the
/// destination file (E-D4, snapshot schema 4).
///
/// The password is **not** here and never will be. What is here is what the
/// headers already state in the clear plus two things this process computed:
/// the retained tail padding, and the cipher checkpoints that let a resumed
/// download decrypt at a coverage frontier without re-encrypting the member
/// from its start.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MemberCryptSnapshot {
    pub(crate) salt: [u8; 16],
    pub(crate) kdf_count_lg2: u8,
    pub(crate) iv: [u8; 16],
    /// Whether the header claims a password-check value. Compared against the
    /// re-parsed facts at restore: a row that disagrees with the headers is a
    /// row describing a different archive.
    pub(crate) psw_check_present: bool,
    /// Whether the member's whole-member checksum is a keyed fold.
    pub(crate) data_hash_uses_mac: bool,
    pub(crate) cipher_size: u64,
    pub(crate) tail_padding: u8,
    /// The ≤15 plaintext bytes past `unpacked_size`. Empty until the member's
    /// final block has been decrypted.
    pub(crate) tail_plain: Vec<u8>,
    /// `(cipher offset, the 16 cipher bytes ending there)`, one per contiguous
    /// decrypted run — which for an ordinary download is exactly one.
    pub(crate) checkpoints: Vec<(u64, [u8; 16])>,
}

/// Why a restored member's crypt facts were refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CryptRestoreError {
    /// The stored facts are not the facts the rebuilt layout states.
    FactsDisagree,
    /// The row claims more padding than a block can hold, or padding bytes that
    /// do not match the size it also claims.
    Malformed,
}

/// One encrypted member's write-side transform state (E-D2).
#[derive(Debug)]
pub(crate) struct MemberCrypt {
    keys: MemberKeys,
    /// The member's own AES-CBC IV, from its `FHEXTRA_CRYPT` record. It is the
    /// predecessor of cipher block 0 and of nothing else.
    iv: [u8; 16],
    salt: [u8; 16],
    kdf_count_lg2: u8,
    psw_check_present: bool,
    /// `align16(unpacked_size)` — how far the member's *cipher* stream runs, and
    /// therefore what every extent and length check on this member must use.
    /// `None` while no header has declared a size.
    cipher_size: Option<u64>,
    /// `cipher_size - unpacked_size`, always `0..16`.
    tail_padding: u8,
    /// Plaintext of a cipher block whose bytes span two source volumes (or two
    /// articles), only part of which has been emitted.
    ///
    /// This is what keeps a split member's straddling block from deadlocking:
    /// each side's drain emits only its own source bytes, and whichever side
    /// decrypts the block first leaves the plaintext here for the other. Keyed
    /// by the block's cipher offset, dropped the moment every byte of it has
    /// been emitted, and bounded by the number of coverage-run boundaries.
    edge_plain: BTreeMap<u64, [u8; 16]>,
    /// The 16 cipher bytes **ending** at the key: the CBC predecessor a span
    /// starting there needs. Kept at the frontier of each contiguous decrypted
    /// run, so an ordinary in-order download holds exactly one.
    ///
    /// Retained rather than re-derived because the cipher is gone once the
    /// plaintext is written: recovering block *N−1* from the destination would
    /// mean re-encrypting the member from its IV.
    checkpoints: BTreeMap<u64, [u8; 16]>,
    /// Cipher ranges this process has decrypted. Only the run *ends* are read —
    /// they are what a checkpoint is allowed to sit at.
    decrypted: ByteRanges,
    /// Cipher ranges whose plaintext has been emitted to a destination (or, for
    /// the padding, retained). An edge block leaves `edge_plain` when this
    /// covers all of it.
    emitted: ByteRanges,
    /// Layer 2's composition, over **plaintext**, in member-logical space.
    ///
    /// Member-wide rather than per part, unlike the plaintext path: layer 1
    /// needs per-part values because part boundaries are where the packed
    /// hashes live, and for an encrypted member those cover cipher bytes. The
    /// plaintext gate composes `[0, unpacked_size)` once and has no per-part
    /// question to answer — and could not use part boundaries anyway, since an
    /// encrypted member's parts are not block-aligned.
    plain_runs: CrcRuns,
    /// The ≤15 plaintext bytes past `unpacked_size` (E-D2). Never written to the
    /// destination; retained because re-encrypting the final block needs them.
    tail_plain: Vec<u8>,
}

impl MemberCrypt {
    pub(crate) fn new(keys: MemberKeys, facts: &RarVolumeMemberEncryptionFacts) -> Self {
        Self {
            keys,
            iv: facts.iv,
            salt: facts.salt,
            kdf_count_lg2: facts.kdf_count_lg2,
            psw_check_present: facts.psw_check_present,
            cipher_size: None,
            tail_padding: 0,
            edge_plain: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            decrypted: ByteRanges::new(),
            emitted: ByteRanges::new(),
            plain_runs: CrcRuns::default(),
            tail_plain: Vec::new(),
        }
    }

    /// Folds in what the newest headers say. The extent can only *resolve* —
    /// from unknown to known — while the member is routing; a member whose
    /// declared size changes has already been classified malformed.
    pub(crate) fn observe(&mut self, facts: &EncryptedStore) {
        if let Some(cipher_size) = facts.cipher_size {
            self.cipher_size = Some(cipher_size);
        }
        if let Some(tail_padding) = facts.tail_padding {
            self.tail_padding = tail_padding;
        }
    }

    /// How far the member's cipher stream runs. **Not** `unpacked_size`: that
    /// number is short by the tail padding, and using it would clip the final
    /// block off every length check.
    pub(crate) fn cipher_size(&self) -> Option<u64> {
        self.cipher_size
    }

    pub(crate) fn plain_runs(&self) -> &CrcRuns {
        &self.plain_runs
    }

    pub(crate) fn plain_runs_mut(&mut self) -> &mut CrcRuns {
        &mut self.plain_runs
    }

    /// The retained padding, for the assertions that pin it. Not a routing
    /// input: the router writes these bytes nowhere, and E2 reads them off the
    /// snapshot row rather than out of here.
    #[cfg(test)]
    pub(crate) fn tail_plain(&self) -> &[u8] {
        &self.tail_plain
    }

    /// The CBC predecessor of the block starting at `block_start`: the member's
    /// IV at offset 0, a retained checkpoint otherwise. `None` means the caller
    /// must find the 16 cipher bytes itself — or hold the span.
    pub(crate) fn preceding_block(&self, block_start: u64) -> Option<[u8; 16]> {
        if block_start == 0 {
            return Some(self.iv);
        }
        self.checkpoints.get(&block_start).copied()
    }

    /// Plaintext of an already-decrypted block whose bytes another volume's
    /// drain has not emitted yet.
    pub(crate) fn edge_plain(&self, block_start: u64) -> Option<[u8; 16]> {
        self.edge_plain.get(&block_start).copied()
    }

    /// Decrypts one block-aligned cipher range in place (E-D2).
    ///
    /// `start` and `cipher.len()` are both multiples of 16 — cipher offset and
    /// member-logical offset are the same number for a stored member — and
    /// `preceding` is the 16 cipher bytes immediately before `start`. Records
    /// the range as decrypted and files the run's trailing cipher block as a
    /// checkpoint **before** the bytes are overwritten, which is the only moment
    /// that cipher exists.
    pub(crate) fn decrypt_range(
        &mut self,
        start: u64,
        preceding: &[u8; 16],
        cipher: &mut [u8],
    ) -> bool {
        if cipher.is_empty() {
            return true;
        }
        let len = cipher.len() as u64;
        debug_assert_eq!(start % AES_BLOCK, 0);
        debug_assert_eq!(len % AES_BLOCK, 0);
        let mut trailing = [0u8; 16];
        trailing.copy_from_slice(&cipher[cipher.len() - 16..]);
        if decrypt_cipher_range(&self.keys.key, preceding, cipher).is_err() {
            return false;
        }
        self.decrypted.insert(start, len);
        self.checkpoints.insert(start + len, trailing);
        self.prune_checkpoints();
        true
    }

    /// Files the plaintext of a block the caller could only emit part of.
    pub(crate) fn retain_edge(&mut self, block_start: u64, plain: [u8; 16]) {
        self.edge_plain.insert(block_start, plain);
    }

    /// Records emitted cipher coverage and drops any edge block it completes.
    /// Returns the bytes this call is the **first** to emit, which is what tells
    /// a duplicate article from a new one in cipher space.
    pub(crate) fn note_emitted(&mut self, start: u64, len: u64) -> u64 {
        if len == 0 {
            return 0;
        }
        let fresh = self.emitted.insert(start, len);
        for block_start in [block_floor(start), block_floor(start + len - 1)] {
            let block_len = self
                .cipher_size
                .map(|size| size.saturating_sub(block_start).min(AES_BLOCK))
                .unwrap_or(AES_BLOCK);
            if block_len > 0 && self.emitted.missing(block_start, block_len).is_empty() {
                self.edge_plain.remove(&block_start);
            }
        }
        fresh
    }

    /// Whether every cipher byte of `[start, start + len)` has been emitted —
    /// the completeness question layer 1 asks about a part, in the only space
    /// that can answer it. The destination coverage map cannot: it stops at
    /// `unpacked_size`, and the final part's packed hash covers the padding too.
    pub(crate) fn emitted_covers(&self, start: u64, len: u64) -> bool {
        self.emitted.missing(start, len).is_empty()
    }

    /// Whether the ≤15 plaintext bytes past the member's end are in hand.
    ///
    /// Read as a **verification precondition** rather than as a coverage
    /// question, because it has to survive a restart: `emitted` is per-process
    /// and the padding is the one part of the cipher stream the destination
    /// coverage map cannot describe (those bytes are not destination bytes). The
    /// snapshot carries the padding itself, so this answers the same way before
    /// and after a restart — and a member that is destination-complete without
    /// it is a member whose final block was never decrypted.
    pub(crate) fn tail_padding_retained(&self) -> bool {
        self.tail_plain.len() as u64 == u64::from(self.tail_padding)
    }

    /// Seeds cipher coverage a previous run emitted (E-D4). Purely a duplicate
    /// filter: it carries no claim that anything was verified, which is what
    /// `restart_seeded` is for.
    pub(crate) fn seed_emitted(&mut self, start: u64, len: u64) {
        self.emitted.insert(start, len);
    }

    /// Keeps the ≤15 plaintext bytes past the member's declared end.
    ///
    /// `offset` is where `plain` starts in member-logical space; only the part
    /// of it at or past `unpacked_size` is kept, and it is kept at its true
    /// position inside the padding so an out-of-order arrival cannot scramble
    /// it.
    pub(crate) fn retain_tail_padding(&mut self, unpacked_size: u64, offset: u64, plain: &[u8]) {
        let padding = u64::from(self.tail_padding);
        if padding == 0 {
            return;
        }
        let end = offset.saturating_add(plain.len() as u64);
        if end <= unpacked_size {
            return;
        }
        let from = unpacked_size.max(offset);
        let take = &plain[(from - offset) as usize..];
        if self.tail_plain.len() < padding as usize {
            self.tail_plain.resize(padding as usize, 0);
        }
        let at = (from - unpacked_size) as usize;
        let room = self.tail_plain.len().saturating_sub(at);
        let take = &take[..take.len().min(room)];
        self.tail_plain[at..at + take.len()].copy_from_slice(take);
    }

    /// Layer 2's comparison (E-D3): the composed plain CRC32 over the member's
    /// plaintext, folded with the KDF hash key when the header keys it.
    ///
    /// This is the **real wrong-password backstop**. Layer 1's packed hashes are
    /// plain CRC32s over cipher bytes on non-final parts, so they pass whatever
    /// the password was; a wrong password that got past admission — the header
    /// carried no check, or carried a forged one — is caught here and nowhere
    /// earlier.
    pub(crate) fn fold_member_crc(&self, composed: u32, uses_mac: bool) -> u32 {
        if uses_mac {
            convert_crc32_to_mac(composed, &self.keys.hash_key)
        } else {
            composed
        }
    }

    /// The snapshot row for this member (E-D4).
    pub(crate) fn snapshot(&self, data_hash_uses_mac: bool) -> Option<MemberCryptSnapshot> {
        Some(MemberCryptSnapshot {
            salt: self.salt,
            kdf_count_lg2: self.kdf_count_lg2,
            iv: self.iv,
            psw_check_present: self.psw_check_present,
            data_hash_uses_mac,
            cipher_size: self.cipher_size?,
            tail_padding: self.tail_padding,
            tail_plain: self.tail_plain.clone(),
            checkpoints: self
                .checkpoints
                .iter()
                .map(|(offset, block)| (*offset, *block))
                .collect(),
        })
    }

    /// Seeds a restored member (E-D4).
    ///
    /// Refuses when the row disagrees with what the rebuilt layout states: the
    /// facts are re-derived from the cached headers on every restart, so a
    /// mismatch is a row describing a different archive, and rebuilding keys
    /// from it would decrypt with the wrong IV or the wrong salt while every
    /// gate carried on passing over ciphertext.
    pub(crate) fn restore(
        &mut self,
        stored: &MemberCryptSnapshot,
        data_hash_uses_mac: bool,
    ) -> Result<(), CryptRestoreError> {
        if stored.salt != self.salt
            || stored.kdf_count_lg2 != self.kdf_count_lg2
            || stored.iv != self.iv
            || stored.psw_check_present != self.psw_check_present
            || stored.data_hash_uses_mac != data_hash_uses_mac
            || self
                .cipher_size
                .is_some_and(|size| size != stored.cipher_size)
            || stored.tail_padding != self.tail_padding
        {
            return Err(CryptRestoreError::FactsDisagree);
        }
        if u64::from(stored.tail_padding) >= AES_BLOCK
            || !stored.cipher_size.is_multiple_of(AES_BLOCK)
            || stored.tail_plain.len() > usize::from(stored.tail_padding)
            || stored.checkpoints.iter().any(|(offset, _)| {
                !offset.is_multiple_of(AES_BLOCK) || *offset > stored.cipher_size
            })
        {
            return Err(CryptRestoreError::Malformed);
        }
        self.cipher_size = Some(stored.cipher_size);
        self.tail_plain = stored.tail_plain.clone();
        for (offset, block) in &stored.checkpoints {
            self.checkpoints.insert(*offset, *block);
            // A checkpoint is a decrypted-run frontier, so the block it names is
            // decrypted by construction. Recording it keeps the pruning rule
            // from throwing the row away on the first live decrypt.
            self.decrypted
                .insert(offset.saturating_sub(AES_BLOCK), AES_BLOCK);
        }
        Ok(())
    }

    /// Keeps one checkpoint per contiguous decrypted run — the frontier a
    /// resumed span will start at. Without it a long download would retain 16
    /// bytes per article for the life of the set.
    fn prune_checkpoints(&mut self) {
        let ends: std::collections::BTreeSet<u64> = self
            .decrypted
            .ranges()
            .iter()
            .map(|(_, end)| *end)
            .collect();
        self.checkpoints.retain(|offset, _| ends.contains(offset));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(psw_check: Option<[u8; 12]>) -> RarVolumeMemberEncryptionFacts {
        RarVolumeMemberEncryptionFacts {
            version: 0,
            kdf_count_lg2: 4,
            salt: [7u8; 16],
            iv: [9u8; 16],
            psw_check_present: psw_check.is_some(),
            psw_check,
        }
    }

    /// The 12-byte field a header would carry for `password`. Only the first 8
    /// bytes matter here — the 4-byte SHA-256 tag is validated by the *parser*,
    /// which is why `RarVolumeMemberEncryptionFacts::psw_check` is already an
    /// `Option` by the time admission sees it.
    fn password_check_for(password: &str, salt: &[u8; 16], lg2: u8) -> [u8; 12] {
        let material = derive_rar5_material(password, salt, lg2).expect("derivable");
        let mut check = [0u8; 12];
        check[..8].copy_from_slice(&material.psw_check);
        check
    }

    #[test]
    fn no_password_refuses_admission() {
        let mut ring = KeyRing::new();
        assert!(ring.wants_password());
        assert_eq!(
            ring.admit(&facts(None)).err(),
            Some(CryptRefusal::NoPassword)
        );
        assert!(!ring.admitted());
    }

    #[test]
    fn a_wrong_password_with_a_check_present_refuses_admission() {
        let facts = facts(Some(password_check_for("right", &[7u8; 16], 4)));
        let mut ring = KeyRing::new();
        ring.set_password(Some("wrong"));
        assert_eq!(ring.admit(&facts).err(), Some(CryptRefusal::WrongPassword));
        assert_eq!(ring.refusal(), Some(CryptRefusal::WrongPassword));
        // Sticky: a second parse reaches the same verdict rather than
        // re-deriving its way to a different one.
        assert_eq!(ring.admit(&facts).err(), Some(CryptRefusal::WrongPassword));
    }

    #[test]
    fn a_wrong_password_with_no_check_admits_provisionally() {
        let mut ring = KeyRing::new();
        ring.set_password(Some("wrong"));
        assert!(ring.admit(&facts(None)).is_ok());
        assert!(ring.admitted());
    }

    #[test]
    fn one_derivation_per_crypt_tuple() {
        let mut ring = KeyRing::new();
        ring.set_password(Some("right"));
        let first = ring.admit(&facts(None)).expect("admitted");
        let second = ring.admit(&facts(None)).expect("admitted");
        assert_eq!(first.key, second.key);
        assert_eq!(ring.keys.len(), 1);
    }

    #[test]
    fn checkpoints_keep_one_entry_per_contiguous_run() {
        let mut crypt = MemberCrypt::new(
            MemberKeys {
                key: [1u8; 32],
                hash_key: [2u8; 32],
            },
            &facts(None),
        );
        crypt.cipher_size = Some(96);
        let mut bytes = vec![0u8; 32];
        assert!(crypt.decrypt_range(0, &[0u8; 16], &mut bytes));
        assert_eq!(crypt.checkpoints.keys().copied().collect::<Vec<_>>(), [32]);
        // A gap keeps its own frontier.
        let mut bytes = vec![0u8; 16];
        assert!(crypt.decrypt_range(64, &[0u8; 16], &mut bytes));
        assert_eq!(
            crypt.checkpoints.keys().copied().collect::<Vec<_>>(),
            [32, 80]
        );
        // Filling the gap merges the runs and retires the interior frontier.
        let mut bytes = vec![0u8; 32];
        assert!(crypt.decrypt_range(32, &[0u8; 16], &mut bytes));
        assert_eq!(crypt.checkpoints.keys().copied().collect::<Vec<_>>(), [80]);
    }

    #[test]
    fn tail_padding_is_kept_at_its_position_and_never_past_it() {
        let mut crypt = MemberCrypt::new(
            MemberKeys {
                key: [1u8; 32],
                hash_key: [2u8; 32],
            },
            &facts(None),
        );
        crypt.cipher_size = Some(48);
        crypt.tail_padding = 5;
        // A run ending inside the member keeps nothing.
        crypt.retain_tail_padding(43, 0, &[0u8; 16]);
        assert!(crypt.tail_plain().is_empty());
        // The final block's plaintext keeps exactly the five bytes past the end.
        let plain: Vec<u8> = (32u8..48).collect();
        crypt.retain_tail_padding(43, 32, &plain);
        assert_eq!(crypt.tail_plain(), &[43, 44, 45, 46, 47]);
    }

    #[test]
    fn a_restored_row_that_disagrees_with_the_headers_is_refused() {
        let facts = facts(None);
        let mut crypt = MemberCrypt::new(
            MemberKeys {
                key: [1u8; 32],
                hash_key: [2u8; 32],
            },
            &facts,
        );
        crypt.cipher_size = Some(48);
        crypt.tail_padding = 5;
        let good = MemberCryptSnapshot {
            salt: facts.salt,
            kdf_count_lg2: facts.kdf_count_lg2,
            iv: facts.iv,
            psw_check_present: false,
            data_hash_uses_mac: true,
            cipher_size: 48,
            tail_padding: 5,
            tail_plain: vec![1, 2, 3, 4, 5],
            checkpoints: vec![(32, [3u8; 16])],
        };
        assert!(crypt.restore(&good, true).is_ok());
        assert_eq!(crypt.preceding_block(32), Some([3u8; 16]));

        let mut wrong_iv = good.clone();
        wrong_iv.iv = [0u8; 16];
        assert_eq!(
            crypt.restore(&wrong_iv, true),
            Err(CryptRestoreError::FactsDisagree)
        );
        let mut wrong_fold = good.clone();
        wrong_fold.data_hash_uses_mac = false;
        assert_eq!(
            crypt.restore(&wrong_fold, true),
            Err(CryptRestoreError::FactsDisagree)
        );
        let mut unaligned = good.clone();
        unaligned.checkpoints = vec![(33, [3u8; 16])];
        assert_eq!(
            crypt.restore(&unaligned, true),
            Err(CryptRestoreError::Malformed)
        );
    }
}
