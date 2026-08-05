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
//!
//! # The read side (E2): posted bytes are re-derived, never stored
//!
//! Everything above is the write side. Phase E2 adds the inverse — every
//! consumer that needs the bytes as they were **posted** gets cipher, which
//! nothing on disk holds any more. AES-CBC encryption is deterministic given
//! key, IV and plaintext, so the posted stream is always reproducible: the
//! provider overlay reads a member's plaintext back out of its `.direct.partial`
//! and re-encrypts it. [`MemberCipher`] is the read-side facts that makes that
//! possible, and [`MemberCrypt::cipher_facts`] is where the write side hands
//! them over.
//!
//! Two things are not re-derivable and are therefore kept:
//!
//! - the **tail padding's plaintext**, because the final block's cipher covers
//!   bytes past `unpacked_size` that the destination never received;
//! - a **CBC seed** for any offset a read starts at that is not the member's
//!   start. Block *N*'s cipher needs block *N−1*'s, so re-encrypting from an
//!   interior offset needs 16 cipher bytes that only existed while the wire
//!   bytes were in hand. [`MemberCrypt::checkpoints`] retains them: at every
//!   contiguous decrypted run's frontier, and additionally every
//!   [`CHECKPOINT_STRIDE`] bytes inside one.
//!
//! The stride is E2's answer to plan 136's open question 1. A run frontier alone
//! is the wrong shape for a *ranged* read: an ordinary in-order download keeps
//! exactly one checkpoint and it sits at the download frontier, which is past
//! every interior offset a verifier or a repair ever asks about — so every
//! ranged read would chain from the member's start and a slice-by-slice sweep
//! would be quadratic in the member's size. The stride bounds that chain to one
//! stride's worth of AES, costs 24 bytes per stride per member in memory and in
//! the snapshot, and needs no second in-memory tier on top.
//!
//! # The one precondition the overlay must not assume (E1 review F6)
//!
//! [`MemberCrypt::edge_plain`] and [`MemberCrypt::checkpoints`] are only ever
//! valid for the bytes that produced them. A **repair** rewrites a span in
//! place: the router's drain re-enters with `replace = true`, the destination
//! takes the repaired plaintext, and the composition is overwritten — but a
//! cached edge block covering the same offsets still holds the plaintext of the
//! *damaged* bytes, and a checkpoint at that frontier still holds the damaged
//! cipher. E1 was correct without touching them because nothing read either one
//! afterwards: the destination is the only consumer of a decrypted edge block,
//! and it has already been written.
//!
//! E2 reads both, so it may not carry that assumption over. Re-encrypting a
//! repaired edge block from `edge_plain`, or chaining a re-encryption from a
//! `checkpoints` entry a repair has invalidated, would emit cipher for bytes the
//! volume no longer holds — silently, because the values stay structurally
//! well-formed. [`MemberCrypt::invalidate_repaired`] is the discharge of that
//! requirement, and [`super::DirectSetRouter::route_encrypted_slice`] calls it
//! on every `replace` span **before** a byte of that span is resolved.
//!
//! # Privacy note: the retained padding is the user's plaintext
//!
//! Those ≤15 bytes are **decrypted content**, and the coverage snapshot is a
//! row in weaver's database. Nothing else the snapshot carries is: the salt, the
//! IV and the KDF count are what the archive states in the clear, and the cipher
//! checkpoints are ciphertext. The password itself is never written anywhere.
//! The padding is the one deliberate exception, it is bounded at 15 bytes per
//! member, and it exists because E2's byte-exact re-encryption of the final
//! block has no other source for it. See [`MemberCryptSnapshot::tail_plain`].

use std::collections::BTreeMap;

use weaver_unrar::{
    EncryptedStore, KdfCache, MemberCipherKey, MemberKeying, PasswordCheck, RarResult,
    check_member_password, convert_crc32_to_mac, derive_rar5_material,
};

use super::CrcRuns;
use crate::pipeline::direct_store::ByteRanges;

/// AES block size, in the one place this module states it.
pub(crate) const AES_BLOCK: u64 = 16;

/// How far apart the cipher checkpoints a ranged re-encryption can seed from are
/// kept inside one contiguous decrypted run (E2, plan 136 open question 1).
///
/// The run *frontier* checkpoint E1 keeps answers the write side's question —
/// "where does the next arriving span chain from" — and answers the read side's
/// not at all: an in-order download holds exactly one, at the download frontier,
/// which is past every interior offset a verifier or a repair asks about. Every
/// ranged read would then chain from the member's start, and a slice-by-slice
/// sweep of an *n*-byte member would re-encrypt O(n²) bytes.
///
/// 4 MiB bounds a ranged read's chain to ~2 ms of AES on any machine weaver
/// runs on, and costs 24 bytes per stride per member — 6 KiB per GiB, in memory
/// and in the coverage snapshot alike. Smaller stride, more snapshot; larger
/// stride, more chaining. Nothing else in the system depends on the value.
pub(crate) const CHECKPOINT_STRIDE: u64 = 4 * 1024 * 1024;

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
    /// The headers state key material this build cannot derive from: a RAR5 KDF
    /// count over `weaver-unrar`'s ceiling.
    ///
    /// **Not** a RAR4 member any more (E3). RAR4 file encryption is keyed here
    /// now, and a RAR4 member the *library* cannot key — one of the three
    /// pre-AES ciphers — never reaches admission at all: it classifies as
    /// `Ineligible(Encrypted)` and demotes under `MemberIneligible` instead,
    /// which is the honest place for it, since nothing about it is a crypt
    /// decision this router made.
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

/// Why a **header-encrypted** (`-hp`) set may not route (plan 136, E4). Every
/// variant demotes, and demoting is the pre-E4 floor: the volume materializes
/// byte-exactly and the conventional extractor opens it with a password prompt.
///
/// Deliberately separate from [`CryptRefusal`], which is about a *member's*
/// file-data key. These are about the *archive's* header key, and the two fail
/// for different reasons at different moments — conflating them would make
/// `encrypted_no_password` mean both "this set has no password" and "this set
/// has no password that opens its headers", which are different operational
/// stories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeaderCryptRefusal {
    /// The job offered no password candidate at all. Nothing to try.
    NoPassword,
    /// Candidates were offered and the archive's own check refuted every one.
    ///
    /// A *demotion*, not a failure: the conventional extractor asks the same
    /// list and will fail the same way, which is the parity that makes this
    /// safe.
    NoVerifiedCandidate,
    /// The archive is RAR5 `-hp` but states no password check this build can
    /// use — the writer omitted it, or the value's own SHA-256 tag did not
    /// validate, which refutes nothing for any password.
    ///
    /// **This is the E4 decision that differs from `-p`.** For file-data
    /// encryption, admitting on `Unverifiable` is fine because a wrong key only
    /// corrupts *data*, and the whole-member CRC32 catches it downstream. Here a
    /// wrong key corrupts the **header parse itself**: the layout — which member
    /// lives where, how long it is, what its checksum is — would be derived from
    /// garbage and routed on. The only thing standing between that and the
    /// user's disk would be "garbage will not parse", which is the same 2⁻³²
    /// argument this arc has already rejected once. So an unprovable `-hp` set
    /// refuses.
    Unverifiable,
    /// RAR4/RAR3 `-hp`. Permanent, and not a gap to be filled later:
    /// `parse_rar4_encrypted_headers` derives a fresh key per header from that
    /// header's own 8-byte salt and the format carries **no password-check value
    /// anywhere**, so a wrong password is detected only by walking off the end of
    /// the archive. There is nothing an admission gate could stand on.
    Rar4Headers,
    /// The archive's type-4 record states key material this build will not
    /// derive from: an encryption version it does not implement, or a KDF count
    /// over `weaver-unrar`'s [`weaver_unrar::CRYPT5_KDF_LG2_COUNT_MAX`].
    ///
    /// The count is the *archive's* claim, so this is the ceiling that keeps a
    /// hostile post from choosing how much PBKDF2 an admission costs. The
    /// library enforces it before it even reads the salt; naming the refusal
    /// here is what stops such a volume from also burning
    /// [`super::MAX_HEADER_PREFIX_BYTES`] of staging first.
    Unkeyable,
}

impl HeaderCryptRefusal {
    pub(crate) fn metric(self) -> &'static str {
        match self {
            Self::NoPassword => "header_encrypted_no_password",
            Self::NoVerifiedCandidate => "header_encrypted_wrong_password",
            Self::Unverifiable => "header_encrypted_unverifiable",
            Self::Rar4Headers => "header_encrypted_rar4",
            Self::Unkeyable => "header_encrypted_unkeyable",
        }
    }
}

/// The `-hp` admission gate: prove one of the job's password candidates against
/// the archive's own type-4 check, before a single header is decrypted (E4).
///
/// # Why this is a candidate *list* and not the one password `KeyRing` holds
///
/// `KeyRing` is fed `spec.password`, which is the job's *first* candidate. The
/// `-hp` gate is offered the whole harvest — `Explicit`, `NzbMeta`,
/// `FilenameConvention`, at most one each — because a set whose header key is
/// the NZB-meta password but whose spec carries an operator's guess would
/// otherwise refuse for a password that was sitting right there. The list is
/// bounded by construction at three, so the KDF work is bounded at three
/// derivations however deep the archive asks for.
///
/// # The password is never persisted
///
/// It lives here, in memory, for the life of the set. A restart re-harvests the
/// candidates from the job's NZB and re-proves them; nothing is carried across.
pub(crate) struct HeaderKeyRing {
    /// In offer order, which is the harvest's own priority order.
    candidates: Vec<HeaderPasswordCandidate>,
    /// The candidate the archive's check verified, once one has.
    verified: Option<String>,
    /// Sticky. A refusal is a demotion, and a demoted set does not come back.
    refusal: Option<HeaderCryptRefusal>,
    cache: KdfCache,
}

/// One offered candidate: its value and where it came from, so the log line a
/// refusal writes can say *which* sources were tried without printing any of
/// them.
#[derive(Clone)]
pub(crate) struct HeaderPasswordCandidate {
    pub(crate) source: &'static str,
    pub(crate) value: String,
}

impl std::fmt::Debug for HeaderKeyRing {
    /// Never prints a candidate's value.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HeaderKeyRing")
            .field("candidates", &self.candidates.len())
            .field("verified", &self.verified.is_some())
            .field("refusal", &self.refusal)
            .finish()
    }
}

impl Default for HeaderKeyRing {
    fn default() -> Self {
        Self::new()
    }
}

impl HeaderKeyRing {
    pub(crate) fn new() -> Self {
        Self {
            candidates: Vec::new(),
            verified: None,
            refusal: None,
            cache: KdfCache::new(),
        }
    }

    /// Offers one candidate, de-duplicated by value and ignored once the ring
    /// has answered. Order of offer is order of trial.
    pub(crate) fn offer(&mut self, source: &'static str, value: &str) {
        if self.refusal.is_some() || self.verified.is_some() || value.is_empty() {
            return;
        }
        if self
            .candidates
            .iter()
            .any(|candidate| candidate.value == value)
        {
            return;
        }
        self.candidates.push(HeaderPasswordCandidate {
            source,
            value: value.to_string(),
        });
    }

    /// The password proved against the archive's check, for the header parses
    /// that follow. `None` until one is proved — which is what makes every parse
    /// before that a no-password parse.
    pub(crate) fn password(&self) -> Option<&str> {
        self.verified.as_deref()
    }

    pub(crate) fn refusal(&self) -> Option<HeaderCryptRefusal> {
        self.refusal
    }

    /// Whether this ring would still take a candidate — the same window
    /// `KeyRing::wants_password` opens, for the same reason.
    pub(crate) fn wants_password(&self) -> bool {
        self.verified.is_none() && self.refusal.is_none()
    }

    /// The E4 decision, made **before any header is decrypted**.
    ///
    /// [`weaver_unrar::PasswordCheck`] has three outcomes and only one of them
    /// admits:
    ///
    /// - [`PasswordCheck::Verified`] — admit. The archive's own check
    ///   reproduces from this password's KDF.
    /// - [`PasswordCheck::Wrong`] — try the next candidate.
    /// - [`PasswordCheck::Unverifiable`] — **refuse the set**, and refuse it
    ///   once for the whole ring rather than per candidate. There is no check to
    ///   try anything against, so trying is guessing, and a wrong guess here
    ///   yields a layout parsed out of garbage.
    ///
    /// The middle rule is why `check_data: Option<[u8; 12]>` matters so much:
    /// the library hands out a check value only when its own SHA-256 tag
    /// validates, and `check_member_password(.., None)` answers `Unverifiable`
    /// and never `Verified`. A malformed check therefore refutes *nothing* — so
    /// reading it as a check would make the first candidate tried a false
    /// verify.
    pub(crate) fn resolve(
        &mut self,
        encryption: &weaver_unrar::RarVolumeHeaderEncryption,
    ) -> Result<&str, HeaderCryptRefusal> {
        if let Some(refusal) = self.refusal {
            return Err(refusal);
        }
        if self.verified.is_some() {
            return Ok(self.verified.as_deref().expect("just checked"));
        }
        let facts = match encryption {
            weaver_unrar::RarVolumeHeaderEncryption::Rar5(facts) => facts,
            weaver_unrar::RarVolumeHeaderEncryption::Rar4 => {
                return Err(self.refuse(HeaderCryptRefusal::Rar4Headers));
            }
            // Only ever reached by a caller that asked about a volume whose
            // headers are readable, which has no `-hp` decision to make. Treated
            // as unprovable rather than admitted, because "there is no archive
            // key" is not "this password is the archive key".
            weaver_unrar::RarVolumeHeaderEncryption::None => {
                return Err(self.refuse(HeaderCryptRefusal::Unverifiable));
            }
        };
        let Some(check) = facts.psw_check.as_ref() else {
            return Err(self.refuse(HeaderCryptRefusal::Unverifiable));
        };
        if self.candidates.is_empty() {
            return Err(self.refuse(HeaderCryptRefusal::NoPassword));
        }
        for candidate in &self.candidates {
            match check_member_password(
                &self.cache,
                &candidate.value,
                &facts.salt,
                facts.kdf_count_lg2,
                Some(check),
            ) {
                PasswordCheck::Verified => {
                    self.verified = Some(candidate.value.clone());
                    return Ok(self.verified.as_deref().expect("just assigned"));
                }
                // Unreachable with a `Some(check)` this build accepted, and
                // stated rather than merged into `Wrong`: if the library ever
                // answered `Unverifiable` here it would mean the check it handed
                // out cannot refute anything, and treating that as "try the next
                // one" would end in `NoVerifiedCandidate` — a *wrong-password*
                // story for what is really an unprovable archive.
                PasswordCheck::Unverifiable => {
                    return Err(self.refuse(HeaderCryptRefusal::Unverifiable));
                }
                PasswordCheck::Wrong => {}
            }
        }
        Err(self.refuse(HeaderCryptRefusal::NoVerifiedCandidate))
    }

    fn refuse(&mut self, refusal: HeaderCryptRefusal) -> HeaderCryptRefusal {
        self.refusal.get_or_insert(refusal);
        self.refusal.expect("just inserted")
    }

    /// Which sources were offered, for a refusal log line. Values never leave
    /// this type.
    pub(crate) fn offered_sources(&self) -> Vec<&'static str> {
        self.candidates
            .iter()
            .map(|candidate| candidate.source)
            .collect()
    }
}

/// One member's derived key material. Copied out of `weaver-unrar`'s zeroizing
/// carrier deliberately: the router needs the key for the life of the set, and
/// the alternative is re-running a 2^n PBKDF2 per span.
#[derive(Clone, Copy)]
pub(crate) struct MemberKeys {
    /// The cipher and its key: AES-256 for RAR5, AES-128 for RAR4. Carrying the
    /// width in the value is what lets every transform below be written once.
    pub(crate) key: MemberCipherKey,
    /// The KDF's hash key, for the keyed checksum folds (E-D3).
    ///
    /// `None` for RAR4, and that is a statement rather than an omission: RAR4
    /// has no hash-MAC flag, so a RAR4 header's checksums are *always* bare
    /// CRC32s and there is nothing to fold them with. See
    /// [`MemberCrypt::fold_member_crc`].
    pub(crate) hash_key: Option<[u8; 32]>,
    /// The member's CBC IV — the predecessor of cipher block 0, and of nothing
    /// else.
    ///
    /// Per member, from two different places: RAR5 reads it out of the
    /// `FHEXTRA_CRYPT` record in the clear, RAR4 takes it from the KDF beside
    /// the key. That difference is exactly why it lives here and not in the
    /// derivation cache, which is keyed by KDF tuple — two RAR5 members sharing
    /// a tuple have *different* IVs.
    pub(crate) iv: [u8; 16],
}

impl std::fmt::Debug for MemberKeys {
    /// Never prints key bytes. A key in a log is a key on disk.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MemberKeys(<redacted>)")
    }
}

/// What one derivation produced, cached per KDF tuple.
///
/// An enum rather than a struct of options, so that "a RAR4 member with no
/// derived IV" is not a value anyone has to handle: the two formats produce
/// genuinely different material, and folding them into one shape would have put
/// an `unwrap_or_default()` on the IV — a zero IV decrypts block 0 to garbage
/// and nothing about it looks wrong.
#[derive(Clone, Copy)]
enum DerivedKeys {
    /// RAR5: AES-256 plus the hash key its keyed checksums fold with. The IV is
    /// **not** here — it is per member and comes from the header, so caching one
    /// against a shared tuple would hand the first member's IV to every later
    /// one.
    Rar5 { key: [u8; 32], hash_key: [u8; 32] },
    /// RAR4: AES-128 and the IV the same derivation produced beside it. No hash
    /// key, because RAR4 has no keyed checksum to fold.
    Rar4 { key: [u8; 16], iv: [u8; 16] },
}

/// The identity a derivation is cached under.
///
/// RAR5 salts the *archive* and every member usually shares the tuple, so one
/// PBKDF2 covers a 200-member set. RAR4 salts each **file**, so the tuple is
/// per member in practice — its KDF is far cheaper, which is why that is
/// affordable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CryptTuple {
    Rar5([u8; 16], u8),
    Rar4(Option<[u8; 8]>),
}

impl CryptTuple {
    fn of(keying: &MemberKeying) -> Self {
        match keying {
            MemberKeying::Rar5(facts) => Self::Rar5(facts.salt, facts.kdf_count_lg2),
            MemberKeying::Rar4 { salt } => Self::Rar4(*salt),
        }
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
    /// One derivation per [`CryptTuple`].
    keys: BTreeMap<CryptTuple, DerivedKeys>,
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
    /// seam re-reads it while this is true.
    ///
    /// It stays true while a password is merely *held*, and goes false the
    /// moment one is **admitted** (E1 review F5). The narrower "held" test this
    /// replaces made [`Self::set_password`]'s changed-password branch dead code:
    /// a job added with the wrong password and corrected before its first header
    /// parsed would never see the correction, because the seam stopped asking
    /// the instant any password existed. Costs one map lookup per article for a
    /// job with no password — which is every conventional job — and one short
    /// `String` clone per article for one that has one, until admission.
    pub(crate) fn wants_password(&self) -> bool {
        !self.admitted && self.refusal.is_none()
    }

    /// Binds the job's password. A no-op once the same one is held, and refused
    /// outright once a password has been refuted or admitted: re-admitting on a
    /// *changed* password would need every routed byte re-decrypted, which is a
    /// demotion with extra steps.
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
    /// A PAR2-bearing job used to be refused outright here (E1 review F1),
    /// because an encrypted set's destinations hold plaintext where PAR2
    /// describes the posted cipher and nothing could turn one back into the
    /// other. E2's re-encrypting overlay is what retires that refusal: the
    /// authoritative pass, live verification, repair and reconstruction all read
    /// posted bytes through it now, so a recovery set is no longer a reason to
    /// leave direct mode.
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
    ///
    /// # RAR4 has no third outcome, only the middle one (E3)
    ///
    /// A RAR4 header carries no password-check value at all — the format has no
    /// such field — so every RAR4 member admits on the `Unverifiable` path and
    /// **`WrongPassword` can never fire for one**. That is not a weakening of
    /// this gate; it is the format stating that the member's checksum is the
    /// only wrong-password detector it has, and [`MemberCrypt::fold_member_crc`]
    /// is where it fires. RAR5's absent-check case has always been in exactly
    /// this position, which is why the machinery needed nothing new for it.
    pub(crate) fn admit(&mut self, keying: &MemberKeying) -> Result<MemberKeys, CryptRefusal> {
        if let Some(refusal) = self.refusal {
            return Err(refusal);
        }
        let Some(password) = self.password.clone() else {
            return Err(self.refuse(CryptRefusal::NoPassword));
        };
        let tuple = CryptTuple::of(keying);
        let derived = match self.keys.get(&tuple).copied() {
            Some(derived) => derived,
            None => {
                let derived = self.derive(&password, keying)?;
                self.keys.insert(tuple, derived);
                derived
            }
        };
        self.admitted = true;
        // The tuple encodes the format, so the cache can only ever return the
        // matching variant — but the cross pairs are refused rather than
        // defaulted, because the only way to "handle" them would be to invent a
        // key or an IV, and a wrong one of either decrypts silently.
        match (derived, keying) {
            (DerivedKeys::Rar5 { key, hash_key }, MemberKeying::Rar5(facts)) => Ok(MemberKeys {
                key: MemberCipherKey::Aes256(key),
                hash_key: Some(hash_key),
                iv: facts.iv,
            }),
            (DerivedKeys::Rar4 { key, iv }, MemberKeying::Rar4 { .. }) => Ok(MemberKeys {
                key: MemberCipherKey::Aes128(key),
                hash_key: None,
                iv,
            }),
            _ => Err(self.refuse(CryptRefusal::Unkeyable)),
        }
    }

    /// The derivation itself, once per tuple.
    fn derive(
        &mut self,
        password: &str,
        keying: &MemberKeying,
    ) -> Result<DerivedKeys, CryptRefusal> {
        match keying {
            MemberKeying::Rar5(facts) => {
                match check_member_password(
                    &self.cache,
                    password,
                    &facts.salt,
                    facts.kdf_count_lg2,
                    facts.psw_check.as_ref(),
                ) {
                    PasswordCheck::Wrong => return Err(self.refuse(CryptRefusal::WrongPassword)),
                    PasswordCheck::Verified | PasswordCheck::Unverifiable => {}
                }
                let Ok(mut material) =
                    derive_rar5_material(password, &facts.salt, facts.kdf_count_lg2)
                else {
                    // A KDF count the crate refuses. `check_member_password`
                    // reports the same tuple as `Unverifiable` rather than
                    // `Wrong`, so this is where it is caught: nothing can
                    // decrypt the member at all.
                    return Err(self.refuse(CryptRefusal::Unkeyable));
                };
                let derived = DerivedKeys::Rar5 {
                    key: material.key,
                    hash_key: material.hash_key,
                };
                // The carrier zeroizes on drop; the copies above are the
                // router's.
                material.key.fill(0);
                material.hash_key.fill(0);
                Ok(derived)
            }
            // Infallible by construction: RAR4's KDF has no tunable iteration
            // count to refuse and no check value to refute, and the layout has
            // already established that this member's cipher is AES-128 rather
            // than one of the three pre-AES ones.
            MemberKeying::Rar4 { salt } => {
                let (key, iv) = self.cache.derive_key_rar4(password, salt.as_ref());
                Ok(DerivedKeys::Rar4 { key, iv })
            }
        }
    }

    fn refuse(&mut self, refusal: CryptRefusal) -> CryptRefusal {
        self.refusal.get_or_insert(refusal);
        self.keys.clear();
        refusal
    }
}

/// A member's crypt identity as its **headers** state it, in the shape the
/// coverage snapshot persists (schema 5).
///
/// Deliberately weaver's own type rather than `weaver-unrar`'s
/// [`MemberKeying`]: this one is written to a database row, so its field order
/// is a schema and a library type's is not. [`MemberCryptKeying::of`] is the
/// one-way bridge, and [`MemberCrypt::restore`] compares whole values — a row
/// that disagrees with the rebuilt layout is a row describing a different
/// archive.
///
/// # Why RAR4 persists a salt and not an IV
///
/// RAR5's IV is in the header in the clear, so storing it reveals nothing the
/// archive does not. RAR4's is a **KDF output**, derived from the password
/// beside the key — and RAR4 is precisely the format with *no* password-check
/// value, so persisting a password-derived 16 bytes would put a password
/// verifier in weaver's database that the archive itself deliberately lacks.
/// The row therefore carries the 8-byte file salt (which is in the header) and
/// the restore re-derives the IV from the live password, exactly as the first
/// admission did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum MemberCryptKeying {
    /// RAR5 `FHEXTRA_CRYPT`: AES-256 keyed by PBKDF2 over `salt`/`kdf_count_lg2`.
    Rar5 {
        salt: [u8; 16],
        kdf_count_lg2: u8,
        iv: [u8; 16],
        /// Whether the header claims a password-check value.
        psw_check_present: bool,
    },
    /// RAR4 "RAR 3.0" file encryption: AES-128 keyed by the legacy SHA-1 KDF
    /// over the password and this optional per-file salt.
    Rar4 { salt: Option<[u8; 8]> },
}

impl MemberCryptKeying {
    fn of(keying: &MemberKeying) -> Self {
        match keying {
            MemberKeying::Rar5(facts) => Self::Rar5 {
                salt: facts.salt,
                kdf_count_lg2: facts.kdf_count_lg2,
                iv: facts.iv,
                psw_check_present: facts.psw_check_present,
            },
            MemberKeying::Rar4 { salt } => Self::Rar4 { salt: *salt },
        }
    }
}

/// The crypt facts a restore needs to rebuild a member's keys without
/// re-parsing a header, plus the state that cannot be re-derived from the
/// destination file (E-D4, snapshot schema 5).
///
/// The password is **not** here and never will be. What is here is what the
/// headers already state in the clear plus two things this process computed:
/// the retained tail padding, and the cipher checkpoints that let a resumed
/// download decrypt at a coverage frontier without re-encrypting the member
/// from its start.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MemberCryptSnapshot {
    /// What the headers say keys this member. Compared whole at restore.
    pub(crate) keying: MemberCryptKeying,
    /// Whether the member's whole-member checksum is a keyed fold. Always
    /// `false` for a RAR4 member — the format has no hash-MAC flag — which the
    /// restore comparison enforces for free by comparing it.
    pub(crate) data_hash_uses_mac: bool,
    pub(crate) cipher_size: u64,
    pub(crate) tail_padding: u8,
    /// The ≤15 plaintext bytes past `unpacked_size`.
    ///
    /// Either empty or exactly `tail_padding` long, and never a partially filled
    /// buffer: a row is written only once every one of those bytes has really
    /// arrived (E1 review F4). A padding that is half in hand at checkpoint time
    /// is simply not carried, which costs nothing — the other half of its cipher
    /// block is still outstanding, so the article carrying it comes back and the
    /// whole block is retained in one piece on the resumed run.
    ///
    /// **This is decrypted user content in weaver's database.** Bounded at 15
    /// bytes per member, and the only such field: see the module docs for why it
    /// cannot come from anywhere else.
    pub(crate) tail_plain: Vec<u8>,
    /// `(cipher offset, the 16 cipher bytes ending there)`, one per contiguous
    /// decrypted run — which for an ordinary download is exactly one.
    pub(crate) checkpoints: Vec<(u64, [u8; 16])>,
}

impl std::fmt::Debug for MemberCryptSnapshot {
    /// Withholds `tail_plain`, and nothing else.
    ///
    /// By this type's own account that is the only field here which is neither
    /// clear header material nor ciphertext, so it is the only one worth
    /// withholding: `keying` is what the headers state, and `checkpoints` are
    /// bytes that were already on the wire.
    ///
    /// That this row is deliberately `Serialize`d is not an argument for a
    /// derived `Debug`. Storage and logs are different surfaces with different
    /// readers — the row lives in a database weaver already protects, while a
    /// trace line goes wherever logs go — so persisting those ≤15 bytes on
    /// purpose says nothing about printing them by accident. This is the last
    /// type in the chain that lacked a hand-written impl; the others are
    /// [`MemberKeys`], [`MemberCipher`], [`KeyRing`] and [`MemberCrypt`].
    ///
    /// What is printed is `keying` — which is exactly what a restore refusal is
    /// an argument about — plus shape.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemberCryptSnapshot")
            .field("keying", &self.keying)
            .field("data_hash_uses_mac", &self.data_hash_uses_mac)
            .field("cipher_size", &self.cipher_size)
            .field("tail_padding", &self.tail_padding)
            .field("tail_retained", &(self.tail_plain.len() as u8))
            .field("checkpoints", &self.checkpoints.len())
            .finish_non_exhaustive()
    }
}

/// One encrypted member's **read-side** facts: everything the provider overlay
/// needs to turn the plaintext in a `.direct.partial` back into the bytes that
/// were posted (E-D4, phase E2).
///
/// A snapshot of the write side, taken whenever a provider is assembled, and
/// deliberately not a handle on it: the overlay runs on the blocking pool, often
/// inside `spawn_blocking`, while the router keeps taking articles.
///
/// The key is in here because re-encryption needs it. It is never printed, never
/// serialized and never leaves the process — see [`Self::fmt`].
#[derive(Clone)]
pub(crate) struct MemberCipher {
    /// The cipher and its key. AES-128 for a RAR4 member and AES-256 for a
    /// RAR5 one, chosen where the key was derived rather than here — the
    /// overlay re-encrypts through one call either way.
    key: MemberCipherKey,
    /// The member's own CBC IV: the predecessor of cipher block 0.
    iv: [u8; 16],
    /// Plaintext length — how much of the member's partial is destination bytes,
    /// and where the tail padding starts.
    unpacked_size: u64,
    /// `align16(unpacked_size)`: how far the posted cipher stream runs.
    cipher_size: u64,
    /// The ≤15 plaintext bytes past `unpacked_size`, or `None` when they are not
    /// all in hand.
    ///
    /// `None` is a **refusal**, not an absence: without them the final block
    /// cannot be re-encrypted, and every byte of that block — including the
    /// destination bytes below `unpacked_size` — is unreproducible. Fabricating
    /// them (zeros, say) would produce a structurally valid cipher block that is
    /// not the one that was posted, which PAR2 would report as damage in a
    /// byte-perfect volume.
    tail_plain: Option<Vec<u8>>,
    /// `(cipher offset, the 16 cipher bytes ending there)` — the CBC seeds a
    /// ranged re-encryption can start from. See [`CHECKPOINT_STRIDE`].
    checkpoints: BTreeMap<u64, [u8; 16]>,
    /// The member's destination coverage, in member-logical (== cipher) space.
    ///
    /// The overlay's other precondition, and the one a volume-level coverage map
    /// cannot answer: re-encrypting `[O, O + n)` reads plaintext from the seed
    /// all the way to `O`, which for a split member crosses source volumes
    /// inside one partial file. A gap anywhere in that span is a range the
    /// filesystem answers with zeros, and CBC would turn those zeros into
    /// well-formed cipher for every block from there to the member's end.
    covered: ByteRanges,
}

impl std::fmt::Debug for MemberCipher {
    /// Never prints key bytes, and never prints the retained padding either —
    /// that is decrypted user content.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemberCipher")
            .field("unpacked_size", &self.unpacked_size)
            .field("cipher_size", &self.cipher_size)
            .field("tail_retained", &self.tail_plain.is_some())
            .field("checkpoints", &self.checkpoints.len())
            .finish_non_exhaustive()
    }
}

/// Where a ranged re-encryption may start, and what it costs to get there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CipherSeed {
    /// Block-aligned cipher offset the chain starts at.
    pub(crate) chain_start: u64,
    /// The 16 cipher bytes immediately before `chain_start`.
    pub(crate) preceding: [u8; 16],
}

impl MemberCipher {
    pub(crate) fn unpacked_size(&self) -> u64 {
        self.unpacked_size
    }

    pub(crate) fn cipher_size(&self) -> u64 {
        self.cipher_size
    }

    /// The retained tail padding, or `None` when the member cannot serve any
    /// read that touches its final cipher block.
    pub(crate) fn tail_plain(&self) -> Option<&[u8]> {
        self.tail_plain.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_count(&self) -> usize {
        self.checkpoints.len()
    }

    /// The nearest place a re-encryption reaching `block_start` may chain from:
    /// the member's IV at offset 0, or the greatest retained checkpoint at or
    /// below it.
    ///
    /// Never `None` and never a guess — the IV is always a legitimate seed, and
    /// chaining from it is exactly "fall back to the sequential path". What the
    /// caller learns from `chain_start` is how much plaintext it has to read and
    /// re-encrypt to get there, which is the whole cost of a checkpoint miss.
    pub(crate) fn seed(&self, block_start: u64) -> CipherSeed {
        debug_assert_eq!(block_start % AES_BLOCK, 0);
        match self
            .checkpoints
            .range(..=block_start)
            .next_back()
            .map(|(offset, block)| (*offset, *block))
        {
            Some((chain_start, preceding)) => CipherSeed {
                chain_start,
                preceding,
            },
            None => CipherSeed {
                chain_start: 0,
                preceding: self.iv,
            },
        }
    }

    /// Whether every byte of `[start, end)` really is in the member's partial.
    ///
    /// Clamped at `unpacked_size`, because the coverage map stops there: the
    /// padding is not destination bytes and is vouched for by
    /// [`Self::tail_plain`] instead.
    pub(crate) fn plaintext_present(&self, start: u64, end: u64) -> bool {
        let end = end.min(self.unpacked_size);
        end <= start || self.covered.missing(start, end - start).is_empty()
    }

    /// CBC-encrypts `buffer` **in place** — a whole number of blocks whose
    /// plaintext starts immediately after `preceding` — back into the bytes that
    /// were posted.
    ///
    /// The inverse of [`MemberCipherKey::decrypt_range`], and deliberately the
    /// *same* backend: `weaver-unrar` picks AWS-LC or the pure-Rust cipher per
    /// target and pins the two equal with differential tests — for both widths —
    /// so re-encrypting through it cannot drift from the decrypt weaver already
    /// trusts.
    ///
    /// In place, and **fallible**, for two reasons the review named (E2, F5 and
    /// the test-support note). In place because the overlay's caller already
    /// owns a buffer the plaintext was read into, and returning a fresh `Vec`
    /// meant copying every re-encrypted byte twice. Fallible because the caller
    /// is a reader on the blocking pool: a violated length contract has to come
    /// back as a hole it can report as unavailable bytes, never as a panic
    /// inside a `spawn_blocking` task. The contract holds by construction —
    /// `cipher_size` is `align16` and every range here is block-derived — so
    /// what is at stake is the failure *mode*, not a live failure.
    pub(crate) fn encrypt(&self, preceding: &[u8; 16], buffer: &mut [u8]) -> RarResult<()> {
        self.key.encrypt_range(preceding, buffer)
    }
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
///
/// Its `Debug` is hand-written and withholds three things — see [`Self::fmt`].
pub(crate) struct MemberCrypt {
    keys: MemberKeys,
    /// The member's own AES-CBC IV — its `FHEXTRA_CRYPT` record's for RAR5, the
    /// KDF's for RAR4. It is the predecessor of cipher block 0 and of nothing
    /// else.
    iv: [u8; 16],
    /// What the headers say keys this member, in the shape the snapshot
    /// persists. The restore comparison is over this whole value.
    keying: MemberCryptKeying,
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
    ///
    /// **Decrypted user content**, and the only such state this module keeps —
    /// it is also the only thing in the coverage snapshot that is not either
    /// clear header material or ciphertext. See the module docs.
    tail_plain: Vec<u8>,
    /// Which bytes of `tail_plain` have actually arrived: bit *i* is padding
    /// byte *i*. `tail_plain` is resized to the full padding on the first
    /// arrival, so its length says nothing about how much of it is real, and
    /// [`Self::tail_padding_retained`] is a **verification precondition** —
    /// answering it off a zero-filled buffer would let a member verify against
    /// padding it never saw (E1 review F4).
    tail_filled: u16,
}

impl std::fmt::Debug for MemberCrypt {
    /// Never prints key material, and never prints decrypted content either.
    ///
    /// Three fields are withheld. `keys` carries the member's key, for the same
    /// reason [`MemberKeys`] redacts its own. `iv` is withheld because for a
    /// **RAR4** member those 16 bytes are a KDF output — password-derived, not
    /// header material as they are for RAR5 — which is exactly why
    /// [`MemberCryptKeying`] refuses to persist them: a format with no
    /// password-check value should not gain a password verifier from weaver.
    /// Leaving them in a derived `Debug` would hand one back. `edge_plain` and
    /// `tail_plain` are the user's plaintext, held here only until it has been
    /// emitted (or, for the padding, only because E2 cannot re-derive it).
    ///
    /// What is printed is shape: how far the cipher stream runs, how much of it
    /// this member is holding, and whether the padding is whole — the questions
    /// a routing trace actually asks.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemberCrypt")
            .field("cipher_size", &self.cipher_size)
            .field("tail_padding", &self.tail_padding)
            .field("tail_retained", &self.tail_padding_retained())
            .field("edge_blocks", &self.edge_plain.len())
            .field("checkpoints", &self.checkpoints.len())
            .finish_non_exhaustive()
    }
}

impl MemberCrypt {
    pub(crate) fn new(keys: MemberKeys, keying: &MemberKeying) -> Self {
        Self {
            iv: keys.iv,
            keys,
            keying: MemberCryptKeying::of(keying),
            cipher_size: None,
            tail_padding: 0,
            edge_plain: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            decrypted: ByteRanges::new(),
            emitted: ByteRanges::new(),
            plain_runs: CrcRuns::default(),
            tail_plain: Vec::new(),
            tail_filled: 0,
        }
    }

    /// The all-ones mask for `tail_padding` bytes. `tail_padding` is `0..16` by
    /// construction (`cipher_size - unpacked_size`), and a restored row claiming
    /// otherwise is refused as malformed before it reaches here.
    fn tail_mask(&self) -> u16 {
        (1u16 << u32::from(self.tail_padding.min(15))) - 1
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
        // Taken before the decrypt, which is the only moment this cipher exists:
        // the run's frontier block (the write side's seed) plus one block per
        // stride boundary the range crosses (the read side's, E2). Both are
        // stored under "the cipher offset the block ends at", so a lookup keyed
        // by a block start finds the predecessor it needs.
        let mut strided: Vec<(u64, [u8; 16])> = Vec::new();
        let first = start
            .div_ceil(CHECKPOINT_STRIDE)
            .saturating_mul(CHECKPOINT_STRIDE)
            .max(CHECKPOINT_STRIDE);
        let mut at = first;
        while at < start + len {
            if at >= start + AES_BLOCK {
                let offset = (at - start) as usize;
                let mut block = [0u8; 16];
                block.copy_from_slice(&cipher[offset - 16..offset]);
                strided.push((at, block));
            }
            at = at.saturating_add(CHECKPOINT_STRIDE);
        }
        if self.keys.key.decrypt_range(preceding, cipher).is_err() {
            return false;
        }
        self.decrypted.insert(start, len);
        self.checkpoints.insert(start + len, trailing);
        self.checkpoints.extend(strided);
        self.prune_checkpoints();
        true
    }

    /// Discharges E1 review F6: forgets every cached edge block and every
    /// checkpoint a repaired span's cipher range touches.
    ///
    /// Both caches describe the bytes that produced them, and a `replace` span
    /// is the router being told those bytes are gone. Neither value goes stale
    /// in a way anything can detect — a 16-byte plaintext block and a 16-byte
    /// cipher block are structurally valid whatever they hold — so the only
    /// defence is to drop them before anything reads them. The repaired span's
    /// own decrypt re-files whatever it is entitled to re-file.
    ///
    /// Called on the way *in*, before the span is resolved: `route_encrypted_slice`
    /// asks `edge_plain` for the head and tail blocks it cannot decrypt alone,
    /// and would otherwise be handed the plaintext of the damage it is replacing.
    pub(crate) fn invalidate_repaired(&mut self, cipher_offset: u64, len: u64) {
        if len == 0 {
            return;
        }
        let from = block_floor(cipher_offset);
        let to = block_ceil(cipher_offset.saturating_add(len));
        self.edge_plain
            .retain(|start, _| *start < from || *start >= to);
        // A checkpoint at `k` is the block `[k - 16, k)`, so it survives exactly
        // when that block does not overlap the rewrite.
        self.checkpoints
            .retain(|end, _| *end <= from || end.saturating_sub(AES_BLOCK) >= to);
        // The rewritten range is no longer decrypted by this process, so it can
        // neither hold a checkpoint of its own nor keep one alive through
        // `prune_checkpoints`. The span's own `decrypt_range` puts back exactly
        // what it re-derived.
        self.decrypted = super::subtract(&self.decrypted, from, to.saturating_sub(from));
        self.prune_checkpoints();
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

    /// Whether the ≤15 plaintext bytes past the member's end are **all** in
    /// hand.
    ///
    /// Read as a **verification precondition** rather than as a coverage
    /// question, because it has to survive a restart: `emitted` is per-process
    /// and the padding is the one part of the cipher stream the destination
    /// coverage map cannot describe (those bytes are not destination bytes). The
    /// snapshot carries the padding itself, so this answers the same way before
    /// and after a restart — and a member that is destination-complete without
    /// it is a member whose final block was never decrypted.
    ///
    /// Answered off the filled mask, not off `tail_plain.len()` (E1 review F4):
    /// [`Self::retain_tail_padding`] resizes the buffer to the whole padding on
    /// the *first* byte, so its length is true from the first arrival onwards
    /// and would report a split arrival retained while the gaps were still
    /// zeros — and `snapshot` would then persist those zeros as if they were the
    /// member's own bytes.
    pub(crate) fn tail_padding_retained(&self) -> bool {
        self.tail_filled == self.tail_mask()
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
    /// it. Each byte's arrival is recorded in `tail_filled`, which is what
    /// [`Self::tail_padding_retained`] answers from.
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
        // Clamped at the mask's width rather than at `tail_plain.len()`: the
        // padding is `0..16` by construction and a restored row claiming
        // otherwise is refused, so this can only ever be the loop bound — but a
        // shift past 15 would be a panic rather than a wrong answer, and a
        // header field is not something to take a panic on.
        for index in at..(at + take.len()).min(AES_BLOCK as usize - 1) {
            self.tail_filled |= 1u16 << (index as u32);
        }
    }

    /// Layer 2's comparison (E-D3): the composed plain CRC32 over the member's
    /// plaintext, folded with the KDF hash key when the header keys it.
    ///
    /// This is the **real wrong-password backstop**. Layer 1's packed hashes are
    /// plain CRC32s over cipher bytes on non-final parts, so they pass whatever
    /// the password was; a wrong password that got past admission — the header
    /// carried no check, or carried a forged one, or the archive is RAR4 and has
    /// no check to carry — is caught here and nowhere earlier.
    ///
    /// `uses_mac` is the header's own flag, and for a **RAR4** member it is
    /// always `false`: RAR4 has no tweaked-checksum flag, so its whole-member
    /// CRC32 is the bare plaintext one and folding it would compare against a
    /// value the archive never wrote.
    ///
    /// `None` is the fourth combination refusing to answer: a header claiming a
    /// keyed checksum on a member with no hash key is a contradiction between
    /// two facts, and there is no value this could return that means anything.
    /// `weaver-unrar` hard-wires `data_hash_uses_mac = false` on every RAR4
    /// facts path, so the combination is unreachable and this is a shape
    /// statement rather than a live guard — but the alternative was returning
    /// the *unfolded* value and trusting it not to collide with the MAC the
    /// header wrote, which rejects with probability 1 − 2⁻³² instead of
    /// rejecting. Every caller compares against `Some(expected)`, so a refusal
    /// is a mismatch and a mismatch is a demotion.
    pub(crate) fn fold_member_crc(&self, composed: u32, uses_mac: bool) -> Option<u32> {
        match (uses_mac, self.keys.hash_key.as_ref()) {
            (true, Some(hash_key)) => Some(convert_crc32_to_mac(composed, hash_key)),
            (false, _) => Some(composed),
            (true, None) => None,
        }
    }

    /// The snapshot row for this member (E-D4).
    ///
    /// The padding is carried **only** once every byte of it has arrived (E1
    /// review F4). A half-filled buffer is zeros in the gaps, and a row of zeros
    /// is worse than no row: the resumed run would take it for the member's own
    /// bytes and E2 would re-encrypt the final block from them. Dropping it
    /// costs nothing, because a padding that is not whole means the rest of its
    /// cipher block never arrived, so that article is still outstanding and
    /// brings the whole block back in one piece.
    pub(crate) fn snapshot(&self, data_hash_uses_mac: bool) -> Option<MemberCryptSnapshot> {
        Some(MemberCryptSnapshot {
            keying: self.keying,
            data_hash_uses_mac,
            cipher_size: self.cipher_size?,
            tail_padding: self.tail_padding,
            tail_plain: match self.tail_padding_retained() {
                true => self.tail_plain.clone(),
                false => Vec::new(),
            },
            checkpoints: self
                .checkpoints
                .iter()
                .map(|(offset, block)| (*offset, *block))
                .collect(),
        })
    }

    /// The read-side facts for this member (E2), or `None` when it cannot serve
    /// posted bytes at all.
    ///
    /// `None` on a member whose headers have not yet declared a size: nothing
    /// has routed either, so there is nothing to serve — but a caller that
    /// assembled an overlay without noticing would answer that member's extents
    /// out of the *plaintext*, which is the one failure this whole phase exists
    /// to prevent. Refusing here is what lets
    /// [`super::DirectSetRouter::posted_bytes_unavailable`] be a set-level
    /// question with a yes/no answer.
    ///
    /// `covered` is the member's destination coverage, passed in rather than
    /// held here: the crypt state tracks *cipher* coverage (which includes the
    /// padding, and so cannot be compared against a plaintext extent), while the
    /// overlay reads plaintext and needs the map that describes the partial.
    pub(crate) fn cipher_facts(
        &self,
        unpacked_size: u64,
        covered: &ByteRanges,
    ) -> Option<MemberCipher> {
        Some(MemberCipher {
            key: self.keys.key,
            iv: self.iv,
            unpacked_size,
            cipher_size: self.cipher_size?,
            tail_plain: self
                .tail_padding_retained()
                .then(|| self.tail_plain[..usize::from(self.tail_padding)].to_vec()),
            checkpoints: self.checkpoints.clone(),
            covered: covered.clone(),
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
        if stored.keying != self.keying
            || stored.data_hash_uses_mac != data_hash_uses_mac
            || self
                .cipher_size
                .is_some_and(|size| size != stored.cipher_size)
            || stored.tail_padding != self.tail_padding
        {
            return Err(CryptRestoreError::FactsDisagree);
        }
        // A stored padding is all or nothing (E1 review F4): the writer only
        // emits one it has whole, so a short-but-non-empty row is a torn or
        // hand-made one and cannot be told from a real partial.
        if u64::from(stored.tail_padding) >= AES_BLOCK
            || !stored.cipher_size.is_multiple_of(AES_BLOCK)
            || (!stored.tail_plain.is_empty()
                && stored.tail_plain.len() != usize::from(stored.tail_padding))
            || stored.checkpoints.iter().any(|(offset, _)| {
                !offset.is_multiple_of(AES_BLOCK) || *offset > stored.cipher_size
            })
        {
            return Err(CryptRestoreError::Malformed);
        }
        self.cipher_size = Some(stored.cipher_size);
        self.tail_plain = stored.tail_plain.clone();
        self.tail_filled = match stored.tail_plain.is_empty() {
            true => 0,
            false => self.tail_mask(),
        };
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
    /// resumed span will start at — plus one per [`CHECKPOINT_STRIDE`] inside a
    /// run, which is what a ranged re-encryption seeds from (E2).
    ///
    /// Without the first rule a long download would retain 16 bytes per article
    /// for the life of the set; without the second, every ranged read would
    /// chain from the member's start. Both are answered off `decrypted`, so a
    /// checkpoint describing bytes this process no longer claims — a repair's,
    /// after [`Self::invalidate_repaired`] — is dropped by the same pass.
    fn prune_checkpoints(&mut self) {
        let runs: Vec<(u64, u64)> = self.decrypted.ranges().to_vec();
        self.checkpoints.retain(|offset, _| {
            let strided = offset.is_multiple_of(CHECKPOINT_STRIDE);
            runs.iter().any(|(start, end)| {
                // A frontier checkpoint sits exactly at a run's end; a strided
                // one sits inside a run, and the block it names must be inside
                // it too — the block is `[offset - 16, offset)`.
                if *offset == *end {
                    return true;
                }
                strided && *offset <= *end && offset.saturating_sub(AES_BLOCK) >= *start
            })
        });
    }
}

#[cfg(test)]
mod tests {
    use weaver_unrar::RarVolumeMemberEncryptionFacts;

    use super::*;

    /// Key equality without a `Debug` bound: [`MemberCipherKey`] deliberately
    /// has none, so `assert_eq!` cannot be used on one — a key that reaches a
    /// panic message is a key in a log.
    fn assert_same_key(left: MemberCipherKey, right: MemberCipherKey, what: &str) {
        assert!(left == right, "{what}: keys must match");
    }

    fn assert_different_key(left: MemberCipherKey, right: MemberCipherKey, what: &str) {
        assert!(left != right, "{what}: keys must differ");
    }

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

    /// The RAR5 keying those facts state.
    fn keying(psw_check: Option<[u8; 12]>) -> MemberKeying {
        MemberKeying::Rar5(facts(psw_check))
    }

    /// A RAR4 keying: an 8-byte file salt and nothing else — no KDF count, no
    /// header IV, and no password check to refute anything with.
    fn rar4_keying(salt: Option<[u8; 8]>) -> MemberKeying {
        MemberKeying::Rar4 { salt }
    }

    /// Fixed RAR5 key material, for the tests whose subject is the state machine
    /// rather than the cipher.
    fn rar5_keys(key: [u8; 32], hash_key: [u8; 32]) -> MemberKeys {
        MemberKeys {
            key: MemberCipherKey::Aes256(key),
            hash_key: Some(hash_key),
            iv: facts(None).iv,
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
            ring.admit(&keying(None)).err(),
            Some(CryptRefusal::NoPassword)
        );
        assert!(!ring.admitted());
    }

    #[test]
    fn a_wrong_password_with_a_check_present_refuses_admission() {
        let keying = keying(Some(password_check_for("right", &[7u8; 16], 4)));
        let mut ring = KeyRing::new();
        ring.set_password(Some("wrong"));
        assert_eq!(ring.admit(&keying).err(), Some(CryptRefusal::WrongPassword));
        assert_eq!(ring.refusal(), Some(CryptRefusal::WrongPassword));
        // Sticky: a second parse reaches the same verdict rather than
        // re-deriving its way to a different one.
        assert_eq!(ring.admit(&keying).err(), Some(CryptRefusal::WrongPassword));
    }

    #[test]
    fn a_password_corrected_before_admission_replaces_the_one_it_was_added_with() {
        // E1 review F5. `set_password`'s changed-password branch was dead: the
        // seam that calls it stopped asking the moment *any* password was held,
        // so a job added with the wrong one and corrected before its first
        // header parsed admitted with the stale one and then failed the keyed
        // member gate several gigabytes later.
        let keying = keying(Some(password_check_for("right", &[7u8; 16], 4)));
        let mut ring = KeyRing::new();
        ring.set_password(Some("wrong"));
        assert!(
            ring.wants_password(),
            "a held-but-unadmitted password must not close the window"
        );
        ring.set_password(Some("right"));
        assert!(
            ring.admit(&keying).is_ok(),
            "the correction must be the one"
        );
        assert!(ring.admitted());
        assert!(
            !ring.wants_password(),
            "admission is what closes the window, and it must close it"
        );
        // And it stays closed: re-decrypting every routed byte is a demotion
        // with extra steps.
        ring.set_password(Some("another"));
        assert_eq!(ring.keys.len(), 1);
    }

    #[test]
    fn a_wrong_password_with_no_check_admits_provisionally() {
        let mut ring = KeyRing::new();
        ring.set_password(Some("wrong"));
        assert!(ring.admit(&keying(None)).is_ok());
        assert!(ring.admitted());
    }

    #[test]
    fn one_derivation_per_crypt_tuple() {
        let mut ring = KeyRing::new();
        ring.set_password(Some("right"));
        let first = ring.admit(&keying(None)).expect("admitted");
        let second = ring.admit(&keying(None)).expect("admitted");
        assert_same_key(first.key, second.key, "one tuple, one derivation");
        assert_eq!(ring.keys.len(), 1);
    }

    #[test]
    fn checkpoints_keep_one_entry_per_contiguous_run() {
        let mut crypt = MemberCrypt::new(rar5_keys([1u8; 32], [2u8; 32]), &keying(None));
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

    /// A `MemberCrypt` over `size` cipher bytes with a real derivable key, so
    /// the encrypt/decrypt round trip below is over the cipher weaver actually
    /// uses rather than over a made-up key. Returns the raw AES-256 key beside
    /// it, which is what builds the fixture's posted bytes.
    fn keyed_crypt(size: u64, padding: u8) -> (MemberCrypt, [u8; 32]) {
        let material = derive_rar5_material("moonlit-harbour", &[7u8; 16], 4).expect("derivable");
        let keys = MemberKeys {
            key: MemberCipherKey::Aes256(material.key),
            hash_key: Some(material.hash_key),
            iv: facts(None).iv,
        };
        let mut crypt = MemberCrypt::new(keys, &keying(None));
        crypt.cipher_size = Some(size);
        crypt.tail_padding = padding;
        (crypt, material.key)
    }

    #[test]
    fn checkpoints_are_kept_every_stride_so_a_ranged_read_never_chains_from_zero() {
        // Plan 136 open question 1, answered in the state that produces it. An
        // in-order download decrypts one long run, and the frontier rule alone
        // would leave exactly one checkpoint — at the frontier, past every
        // interior offset a verifier asks about.
        let span = (CHECKPOINT_STRIDE * 2 + 8192) as usize;
        let (mut crypt, key) = keyed_crypt(span as u64, 0);
        let plain: Vec<u8> = (0..span).map(|index| (index % 251) as u8).collect();
        let mut cipher = weaver_unrar::test_support::encrypt_aes256_cbc(&key, &[9u8; 16], &plain);
        assert!(crypt.decrypt_range(0, &[9u8; 16], &mut cipher));
        assert_eq!(
            cipher, plain,
            "the fixture must decrypt to its own plaintext"
        );

        let facts = crypt
            .cipher_facts(span as u64, &{
                let mut covered = ByteRanges::new();
                covered.insert(0, span as u64);
                covered
            })
            .expect("a sized member has read-side facts");
        assert_eq!(
            facts.checkpoint_count(),
            3,
            "two stride boundaries plus the run frontier"
        );
        // A read at the last stride boundary seeds there, not at zero: the chain
        // is bounded by the stride however large the member is.
        let seed = facts.seed(CHECKPOINT_STRIDE * 2);
        assert_eq!(seed.chain_start, CHECKPOINT_STRIDE * 2);
        // And the seed really is the predecessor: re-encrypting from it
        // reproduces the posted bytes exactly.
        let at = (CHECKPOINT_STRIDE * 2) as usize;
        let mut reencrypted = plain[at..at + 4096].to_vec();
        facts
            .encrypt(&seed.preceding, &mut reencrypted)
            .expect("a block-aligned range must not be refused");
        let posted = weaver_unrar::test_support::encrypt_aes256_cbc(&key, &[9u8; 16], &plain);
        assert_eq!(reencrypted, posted[at..at + 4096]);

        // An offset below every checkpoint falls back to the member's IV, which
        // is the sequential path rather than a guess.
        assert_eq!(facts.seed(64).chain_start, 0);
        assert_eq!(facts.seed(64).preceding, [9u8; 16]);
    }

    #[test]
    fn a_repaired_span_invalidates_the_edge_block_and_checkpoint_it_overwrites() {
        // E1 review F6, now due. Both caches stay structurally valid across a
        // repair — a 16-byte block is a 16-byte block — so the only defence is
        // dropping them before the overlay reads them.
        let (mut crypt, key) = keyed_crypt(256, 0);
        let plain: Vec<u8> = (0..256u16).map(|index| (index % 251) as u8).collect();
        let posted = weaver_unrar::test_support::encrypt_aes256_cbc(&key, &[9u8; 16], &plain);
        let last_block: [u8; 16] = posted[240..].try_into().expect("one block");
        let mut cipher = posted.clone();
        assert!(crypt.decrypt_range(0, &[9u8; 16], &mut cipher));
        crypt.retain_edge(48, [0xAB; 16]);
        crypt.retain_edge(160, [0xCD; 16]);
        assert_eq!(crypt.checkpoints.keys().copied().collect::<Vec<_>>(), [256]);
        assert_eq!(crypt.preceding_block(256), Some(last_block));

        // A repair over `[48, 96)` takes the edge block it covers and leaves the
        // frontier checkpoint, whose own block it does not touch.
        crypt.invalidate_repaired(48, 48);
        assert_eq!(
            crypt.edge_plain(48),
            None,
            "the repaired edge block must go"
        );
        assert_eq!(
            crypt.edge_plain(160),
            Some([0xCD; 16]),
            "an edge block the rewrite never touched must survive"
        );
        assert_eq!(
            crypt.preceding_block(256),
            Some(last_block),
            "a checkpoint past the rewrite is still the bytes that produced it"
        );

        // And one whose block the rewrite *does* touch goes with it.
        crypt.invalidate_repaired(240, 16);
        assert_eq!(
            crypt.preceding_block(256),
            None,
            "a checkpoint over rewritten cipher must not survive to seed a re-encryption"
        );
    }

    #[test]
    fn tail_padding_is_kept_at_its_position_and_never_past_it() {
        let mut crypt = MemberCrypt::new(rar5_keys([1u8; 32], [2u8; 32]), &keying(None));
        crypt.cipher_size = Some(48);
        crypt.tail_padding = 5;
        // A run ending inside the member keeps nothing.
        crypt.retain_tail_padding(43, 0, &[0u8; 16]);
        assert!(crypt.tail_plain().is_empty());
        // The final block's plaintext keeps exactly the five bytes past the end.
        let plain: Vec<u8> = (32u8..48).collect();
        crypt.retain_tail_padding(43, 32, &plain);
        assert_eq!(crypt.tail_plain(), &[43, 44, 45, 46, 47]);
        assert!(crypt.tail_padding_retained());
    }

    #[test]
    fn a_split_arrival_padding_is_not_retained_until_every_byte_of_it_is_real() {
        // E1 review F4. `retain_tail_padding` zero-fills the whole padding on
        // the *first* byte, so a length test reports a half-arrived padding
        // retained — and `snapshot` then persists the zeros as if the member had
        // produced them. That is exactly E2's byte-exact final-block input.
        let mut crypt = MemberCrypt::new(rar5_keys([1u8; 32], [2u8; 32]), &keying(None));
        crypt.cipher_size = Some(48);
        crypt.tail_padding = 5;

        // The tail's last two bytes only.
        crypt.retain_tail_padding(43, 46, &[0xAA, 0xBB]);
        assert_eq!(
            crypt.tail_plain().len(),
            5,
            "the buffer is sized for the whole padding on the first arrival, which \
             is what made the length test vacuous"
        );
        assert!(
            !crypt.tail_padding_retained(),
            "three of the five bytes are still zero placeholders"
        );
        assert_eq!(
            crypt.snapshot(true).map(|row| row.tail_plain),
            Some(Vec::new()),
            "a padding that is not whole must not be persisted at all"
        );

        // The rest, and only now is it the member's own padding.
        crypt.retain_tail_padding(43, 43, &[0x11, 0x22, 0x33]);
        assert!(crypt.tail_padding_retained());
        assert_eq!(crypt.tail_plain(), &[0x11, 0x22, 0x33, 0xAA, 0xBB]);
        assert_eq!(
            crypt.snapshot(true).map(|row| row.tail_plain),
            Some(vec![0x11, 0x22, 0x33, 0xAA, 0xBB])
        );
    }

    #[test]
    fn a_restored_row_with_a_short_padding_is_refused_rather_than_half_trusted() {
        // The restore side of F4: the writer emits the padding whole or not at
        // all, so a short-but-non-empty row cannot be told from a torn one.
        let mut crypt = MemberCrypt::new(rar5_keys([1u8; 32], [2u8; 32]), &keying(None));
        crypt.cipher_size = Some(48);
        crypt.tail_padding = 5;
        let row = MemberCryptSnapshot {
            keying: MemberCryptKeying::of(&keying(None)),
            data_hash_uses_mac: true,
            cipher_size: 48,
            tail_padding: 5,
            tail_plain: vec![1, 2, 3],
            checkpoints: Vec::new(),
        };
        assert_eq!(crypt.restore(&row, true), Err(CryptRestoreError::Malformed));

        // An empty one restores and reports the padding as still outstanding,
        // which is the honest answer — the article holding it is coming back.
        let mut empty = row.clone();
        empty.tail_plain = Vec::new();
        assert!(crypt.restore(&empty, true).is_ok());
        assert!(!crypt.tail_padding_retained());

        // And a whole one restores as retained.
        let mut whole = row;
        whole.tail_plain = vec![1, 2, 3, 4, 5];
        assert!(crypt.restore(&whole, true).is_ok());
        assert!(crypt.tail_padding_retained());
    }

    #[test]
    fn a_restored_row_that_disagrees_with_the_headers_is_refused() {
        let mut crypt = MemberCrypt::new(rar5_keys([1u8; 32], [2u8; 32]), &keying(None));
        crypt.cipher_size = Some(48);
        crypt.tail_padding = 5;
        let good = MemberCryptSnapshot {
            keying: MemberCryptKeying::of(&keying(None)),
            data_hash_uses_mac: true,
            cipher_size: 48,
            tail_padding: 5,
            tail_plain: vec![1, 2, 3, 4, 5],
            checkpoints: vec![(32, [3u8; 16])],
        };
        assert!(crypt.restore(&good, true).is_ok());
        assert_eq!(crypt.preceding_block(32), Some([3u8; 16]));

        let mut wrong_iv = good.clone();
        wrong_iv.keying = MemberCryptKeying::Rar5 {
            salt: [7u8; 16],
            kdf_count_lg2: 4,
            iv: [0u8; 16],
            psw_check_present: false,
        };
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

        // Plan 136 E3: a RAR4 row over a RAR5 member is the "different archive"
        // case the comparison exists for, and the discriminant is what catches
        // it. Under v4's flat shape there was no field that could disagree —
        // which is why RAR4 is a schema bump rather than an optional field.
        let mut wrong_format = good;
        wrong_format.keying = MemberCryptKeying::of(&rar4_keying(Some([7u8; 8])));
        assert_eq!(
            crypt.restore(&wrong_format, true),
            Err(CryptRestoreError::FactsDisagree)
        );
    }

    #[test]
    fn a_member_crypts_debug_withholds_its_key_material_and_its_plaintext() {
        // Plan 136 E3 review. Nothing formats a `MemberCrypt` today, so this
        // pins a latent path rather than a live leak — but the state it holds
        // is the reason the pattern exists: a **RAR4** member's IV is a KDF
        // output, and `MemberCryptKeying` refuses to persist one precisely
        // because a format with no password-check value must not gain a
        // password verifier from weaver. A derived `Debug` would hand those
        // same 16 bytes back, alongside the member's decrypted tail padding.
        let mut ring = KeyRing::new();
        ring.set_password(Some("moonlit-harbour"));
        let keying = rar4_keying(Some([0x9Bu8; 8]));
        let keys = ring.admit(&keying).expect("admitted");
        let iv = keys.iv;
        let mut crypt = MemberCrypt::new(keys, &keying);
        crypt.observe(&EncryptedStore {
            format: weaver_unrar::ArchiveFormat::Rar4,
            crypt: None,
            rar4_salt: Some([0x9Bu8; 8]),
            cipher_size: Some(48),
            tail_padding: Some(5),
            resolved: true,
        });
        crypt.retain_tail_padding(43, 43, &[0xD7u8; 5]);
        assert!(crypt.tail_padding_retained(), "the test padding is whole");

        let printed = format!("{crypt:?}");
        assert!(
            !printed.contains(&format!("{iv:?}")),
            "the derived IV must never be printed: {printed}"
        );
        assert!(
            !printed.contains(&format!("{:?}", [0xD7u8; 5])) && !printed.contains("215"),
            "the retained plaintext padding must never be printed: {printed}"
        );
        for withheld in ["keys", "iv", "tail_plain", "edge_plain"] {
            assert!(
                !printed.contains(withheld),
                "`{withheld}` must not appear at all: {printed}"
            );
        }
        // And the shape that is safe to print really is printed, or the type
        // becomes useless to trace and someone re-derives `Debug`.
        assert!(
            printed.contains("cipher_size: Some(48)") && printed.contains("tail_retained: true"),
            "the safe shape must still be readable: {printed}"
        );

        // The persisted twin is the same argument one level up. That this row is
        // deliberately serialized does not license a derived `Debug`: the
        // database and the log stream are different surfaces, so the ≤15 bytes
        // it stores on purpose must still not be printed by accident.
        let row = crypt.snapshot(false).expect("a resolved member snapshots");
        assert_eq!(row.tail_plain, vec![0xD7u8; 5], "the row really carries it");
        let stored = format!("{row:?}");
        assert!(
            !stored.contains(&format!("{:?}", [0xD7u8; 5]))
                && !stored.contains("215")
                && !stored.contains("tail_plain"),
            "the persisted plaintext padding must never be printed: {stored}"
        );
        assert!(
            stored.contains("keying") && stored.contains("tail_retained: 5"),
            "the row's safe shape must still be readable: {stored}"
        );
    }

    #[test]
    fn a_rar4_member_admits_provisionally_because_there_is_nothing_to_refute() {
        // Plan 136 E3. RAR4 carries no password-check value at all, so a wrong
        // password reaches the bytes and the member gate is the only detector —
        // exactly the position a RAR5 member with an omitted check is in.
        let mut ring = KeyRing::new();
        ring.set_password(Some("moonlit-harbour"));
        let right = ring
            .admit(&rar4_keying(Some([0x9Bu8; 8])))
            .expect("a RAR4 member has nothing that can refute a password");
        assert!(ring.admitted());
        assert_eq!(ring.refusal(), None);
        assert!(
            right.hash_key.is_none(),
            "RAR4 has no hash-MAC flag, so there is no hash key to fold with"
        );

        // The key and the IV are both KDF outputs, and both change with the
        // salt — so the cache is keyed by it rather than shared across members.
        let other_salt = ring
            .admit(&rar4_keying(Some([0x11u8; 8])))
            .expect("admitted");
        assert_different_key(right.key, other_salt.key, "a different salt");
        assert_ne!(right.iv, other_salt.iv);
        assert_eq!(ring.keys.len(), 2);

        // And a saltless header is a third, complete description rather than a
        // missing one.
        let saltless = ring.admit(&rar4_keying(None)).expect("admitted");
        assert_different_key(right.key, saltless.key, "a saltless header");
        assert_eq!(ring.keys.len(), 3);

        // The same salt twice is one derivation, which is the whole point of the
        // cache.
        let again = ring
            .admit(&rar4_keying(Some([0x9Bu8; 8])))
            .expect("admitted");
        assert_same_key(again.key, right.key, "the same salt twice");
        assert_eq!(again.iv, right.iv);
        assert_eq!(ring.keys.len(), 3);
    }

    #[test]
    fn a_rar4_members_key_and_iv_are_the_derivation_the_library_states() {
        // The router's key must be the one `weaver-unrar` derives for the same
        // password and salt, or every byte it decrypts is garbage that only the
        // member CRC32 would catch. Held against the library surface directly.
        let mut ring = KeyRing::new();
        ring.set_password(Some("moonlit-harbour"));
        let keys = ring
            .admit(&rar4_keying(Some([0x9Bu8; 8])))
            .expect("admitted");
        let (expected_key, expected_iv) =
            weaver_unrar::rar4_derive_key("moonlit-harbour", Some(&[0x9Bu8; 8]));
        assert_same_key(
            keys.key,
            MemberCipherKey::Aes128(expected_key),
            "the router's key is the library's derivation",
        );
        assert_eq!(keys.iv, expected_iv);
    }

    #[test]
    fn a_rar4_member_decrypts_and_re_encrypts_through_the_same_state_machine() {
        // E1's write transform and E2's read side over AES-128, asserting that
        // nothing about either is RAR5-shaped: one `MemberCrypt`, decrypted in
        // out-of-order pieces, whose `cipher_facts` re-encrypt back to the
        // posted bytes from a checkpoint seed.
        const PAYLOAD: usize = 3000;
        let cipher_len = PAYLOAD.div_ceil(16) * 16;
        let mut ring = KeyRing::new();
        ring.set_password(Some("moonlit-harbour"));
        let keying = rar4_keying(Some([0x9Bu8; 8]));
        let keys = ring.admit(&keying).expect("admitted");
        let (raw_key, iv) = weaver_unrar::rar4_derive_key("moonlit-harbour", Some(&[0x9Bu8; 8]));

        let mut padded: Vec<u8> = (0..PAYLOAD).map(|index| (index % 251) as u8).collect();
        for index in PAYLOAD..cipher_len {
            padded.push(0xE0 | (index % 16) as u8);
        }
        // The public range API rather than a `#[doc(hidden)]` test helper: the
        // fixture's "posted" bytes should come from the same surface the overlay
        // re-derives them with (E2 review's test-support finding).
        let mut posted = padded.clone();
        weaver_unrar::encrypt_cipher_range_rar4(&raw_key, &iv, &mut posted)
            .expect("the padded payload is block-aligned");

        let mut crypt = MemberCrypt::new(keys, &keying);
        crypt.cipher_size = Some(cipher_len as u64);
        crypt.tail_padding = (cipher_len - PAYLOAD) as u8;

        // Decrypt in 256-byte pieces, each seeded by its own predecessor block —
        // the E-D2 property, over RAR4.
        let mut covered = ByteRanges::new();
        let mut at = 0usize;
        while at < cipher_len {
            let step = 256.min(cipher_len - at);
            let preceding: [u8; 16] = match at {
                0 => iv,
                _ => posted[at - 16..at].try_into().expect("a whole block"),
            };
            let mut piece = posted[at..at + step].to_vec();
            assert!(crypt.decrypt_range(at as u64, &preceding, &mut piece));
            assert_eq!(
                piece,
                padded[at..at + step],
                "the write transform must reproduce the member's plaintext"
            );
            crypt.retain_tail_padding(PAYLOAD as u64, at as u64, &piece);
            let destination = PAYLOAD.saturating_sub(at).min(step);
            if destination > 0 {
                covered.insert(at as u64, destination as u64);
            }
            at += step;
        }
        assert!(crypt.tail_padding_retained());
        assert_eq!(crypt.tail_plain(), &padded[PAYLOAD..]);

        // And the read side hands the posted bytes back, from a seed rather than
        // from the member's start.
        let facts = crypt
            .cipher_facts(PAYLOAD as u64, &covered)
            .expect("a sized member has read-side facts");
        let seed = facts.seed(1024);
        let mut reencrypted = padded[seed.chain_start as usize..cipher_len].to_vec();
        facts
            .encrypt(&seed.preceding, &mut reencrypted)
            .expect("a block-aligned range must not be refused");
        assert_eq!(
            reencrypted,
            posted[seed.chain_start as usize..],
            "the overlay must reproduce the posted AES-128 stream exactly"
        );

        // The member gate is a *plain* CRC32 for RAR4, which is the only thing
        // it can be: there is no hash key. A header that somehow claimed a
        // keyed checksum is a contradiction the fold refuses outright rather
        // than answering with a value that merely probably rejects — no RAR4
        // facts path can set that flag, so this pins the shape, not a
        // reachable case.
        assert_eq!(crypt.fold_member_crc(0x1234_5678, false), Some(0x1234_5678));
        assert_eq!(crypt.fold_member_crc(0x1234_5678, true), None);
    }
}
