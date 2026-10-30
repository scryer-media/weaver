// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A tracker file: one small TOML file a tracker hands its users, which fills
// one tracker rule. It comes plain, or as the one TOML inside one
// single-volume 7z, ZIP or RAR archive, which may be encrypted and may also
// carry the PEM files of the tracker's SSL identity.
//
// Reading one is pure: bytes and an optional password in, a `TrackerFile` or
// one plain sentence out. Nothing in a tracker file is ever run or fetched,
// and it can neither name the client a torrent presents nor touch the user's
// network routes, bandwidth or limits: every key outside the few below is
// refused by name.
//
// The archive is opened under bounds of its own, far below the bounds of an
// ordinary extraction: a tracker file is a few kilobytes of configuration.

use std::collections::BTreeMap;
use std::io::{Cursor, Read};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

use super::model::{
    ArchivePasswords, Leaf, SeedTimeCounting, SeedingAction, SeedingGoalMode, SeedingPolicy,
    TorrentEncryption, TrackerCertificate, TrackerRule,
};

// The largest upload accepted, in bytes.
pub const MAX_UPLOAD_BYTES: usize = 1 << 20;
// The most an archive may decode to, every member together.
pub const MAX_DECODED_BYTES: u64 = 1 << 20;
// The most entries an archive may hold, directories included.
pub const MAX_ENTRIES: usize = 16;
// The most the archive's headers may take once decoded.
pub const MAX_HEADER_BYTES: u64 = 1 << 20;
// The largest dictionary or model a decoder may allocate.
pub const MAX_DICTIONARY_BYTES: u64 = 128 << 20;
// How many archives may be decoding at once, across every import.
pub const MAX_DECODING: usize = 2;
// A 7z archive's key derivation: 7-Zip's own ceiling of 2^24 rounds per
// derivation, and enough rounds in all for the header's derivation and the
// content's at that ceiling.
const MAX_7Z_AES_CYCLES_POWER: u8 = 24;
const MAX_7Z_KDF_ROUNDS: u64 = 1 << 25;
// The tracker's name and revision as it states them.
const MAX_NAME_BYTES: usize = 128;
const MAX_REVISION_BYTES: usize = 64;

// What a tracker file says, checked.
#[derive(Clone, PartialEq)]
pub struct TrackerFile {
    pub name: String,
    pub revision: String,
    // As `TrackerRule::pattern` keeps it.
    pub host_pattern: String,
    // Values the rule takes and the user may not change while bound.
    pub required: Terms,
    // Values the rule takes until the user changes them.
    pub recommended: Terms,
    // `None` when the file says nothing of them; an empty list clears.
    pub archive_passwords: Option<ArchivePasswords>,
    // Always locked when present.
    pub ssl: Option<TrackerCertificate>,
}

// Neither the passwords nor the identity are ever printed.
impl std::fmt::Debug for TrackerFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackerFile")
            .field("name", &self.name)
            .field("revision", &self.revision)
            .field("host_pattern", &self.host_pattern)
            .field("required", &self.required)
            .field("recommended", &self.recommended)
            .field(
                "archive_passwords",
                &self.archive_passwords.as_ref().map(ArchivePasswords::len),
            )
            .field("ssl", &self.ssl.is_some())
            .finish()
    }
}

// The rule fields one section of a tracker file sets; `None` is not set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Terms {
    pub dht: Option<bool>,
    pub pex: Option<bool>,
    pub lsd: Option<bool>,
    pub encryption: Option<TorrentEncryption>,
    // Only ever recommended: a seeding goal is a stop rule, and a tracker
    // that locked one would hold the user to stopping.
    pub seeding: Option<SeedingTerms>,
}

// The seeding leaves a tracker may recommend, as a rule's seeding policy
// holds them: each inherits while the file does not set it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SeedingTerms {
    pub ratio_goal: Leaf<f32>,
    pub seed_time_goal_minutes: Leaf<u32>,
    pub inactive_goal_minutes: Leaf<u32>,
    pub mode: Leaf<SeedingGoalMode>,
    pub action: Leaf<SeedingAction>,
    pub seed_time_counting: Leaf<SeedTimeCounting>,
}

// The fields of a rule a tracker file can lock or recommend, by the names a
// binding records and a refusal gives.
pub const FIELD_DHT: &str = "dht";
pub const FIELD_PEX: &str = "pex";
pub const FIELD_LSD: &str = "lsd";
pub const FIELD_ENCRYPTION: &str = "encryption";
pub const FIELD_SSL: &str = "ssl";

impl Terms {
    // The names of the fields it sets, seeding aside.
    pub fn fields(&self) -> Vec<&'static str> {
        [
            (FIELD_DHT, self.dht.is_some()),
            (FIELD_PEX, self.pex.is_some()),
            (FIELD_LSD, self.lsd.is_some()),
            (FIELD_ENCRYPTION, self.encryption.is_some()),
        ]
        .into_iter()
        .filter_map(|(field, set)| set.then_some(field))
        .collect()
    }
}

// What a rule remembers of the tracker file that filled it: whose and
// which, when, the fields the file locked, and the values it recommended as
// it recommended them, so a later file can tell a recommendation the user
// left from one the user changed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Binding {
    pub name: String,
    pub revision: String,
    pub imported_at_ms: i64,
    pub locked: Vec<String>,
    #[serde(default)]
    pub recommended: Terms,
}

impl Binding {
    pub fn locks(&self, field: &str) -> bool {
        self.locked.iter().any(|locked| locked == field)
    }
}

impl TrackerFile {
    // The rule this file makes of `rule`, or of no rule when there is none
    // for its host pattern. A required value overwrites and is locked; a
    // recommended one overwrites only a value still as the last file
    // recommended it, so one the user changed stays. An identity in the
    // file replaces the rule's and is locked; a file without one leaves the
    // rule's as it is.
    pub fn apply(&self, rule: Option<&TrackerRule>, now_ms: i64) -> TrackerRule {
        let mut applied = rule.cloned().unwrap_or_else(|| TrackerRule {
            host_pattern: self.host_pattern.clone(),
            ..TrackerRule::default()
        });
        let before = rule
            .and_then(|rule| rule.bound.as_ref())
            .map(|bound| bound.recommended)
            .unwrap_or_default();
        let (required, recommended) = (&self.required, &self.recommended);
        let take = |value: &mut Option<_>, required: Option<_>, recommended, before| {
            *value = required.or(recommend(*value, recommended, before).or(*value));
        };
        take(&mut applied.dht, required.dht, recommended.dht, before.dht);
        take(&mut applied.pex, required.pex, recommended.pex, before.pex);
        take(&mut applied.lsd, required.lsd, recommended.lsd, before.lsd);
        let encryption = recommend(
            applied.encryption,
            recommended.encryption,
            before.encryption,
        );
        applied.encryption = required.encryption.or(encryption).or(applied.encryption);
        // Each seeding leaf the file recommends is taken while the rule's
        // leaf is still what the last file recommended.
        if let Some(seeding) = recommended.seeding {
            let before = before.seeding.unwrap_or_default();
            let mut policy = applied.seeding.unwrap_or_default();
            recommend_leaf(
                &mut policy.ratio_goal,
                seeding.ratio_goal,
                before.ratio_goal,
            );
            recommend_leaf(
                &mut policy.seed_time_goal_minutes,
                seeding.seed_time_goal_minutes,
                before.seed_time_goal_minutes,
            );
            recommend_leaf(
                &mut policy.inactive_goal_minutes,
                seeding.inactive_goal_minutes,
                before.inactive_goal_minutes,
            );
            recommend_leaf(&mut policy.mode, seeding.mode, before.mode);
            recommend_leaf(&mut policy.action, seeding.action, before.action);
            recommend_leaf(
                &mut policy.seed_time_counting,
                seeding.seed_time_counting,
                before.seed_time_counting,
            );
            applied.seeding = SeedingPolicy::kept(Some(policy));
        }
        if let Some(ssl) = &self.ssl {
            applied.ssl = Some(ssl.clone());
        }
        // A list replaces the rule's, an empty one clears it, and a file
        // that names none leaves it.
        if let Some(passwords) = &self.archive_passwords {
            applied.archive_passwords = passwords.clone();
        }
        let mut locked: Vec<String> = required.fields().into_iter().map(str::to_owned).collect();
        if self.ssl.is_some() {
            locked.push(FIELD_SSL.to_owned());
        }
        applied.bound = Some(Binding {
            name: self.name.clone(),
            revision: self.revision.clone(),
            imported_at_ms: now_ms,
            locked,
            recommended: *recommended,
        });
        applied
    }
}

// The recommendation, when the value is still what the last file recommended
// (nothing, when no file did).
fn recommend<T: PartialEq + Copy>(
    current: Option<T>,
    recommended: Option<T>,
    before: Option<T>,
) -> Option<T> {
    recommended.filter(|_| current == before)
}

// A recommended leaf replaces `leaf` while `leaf` is still what the last file
// recommended.
fn recommend_leaf<T: PartialEq + Copy>(leaf: &mut Leaf<T>, recommended: Leaf<T>, before: Leaf<T>) {
    if !recommended.is_inherit() && *leaf == before {
        *leaf = recommended;
    }
}

// Refuses a change `changed` makes to a field the tracker file bound to
// `rule` locked, naming the field and the tracker.
pub fn check_locks(rule: &TrackerRule, changed: &TrackerRule) -> Result<(), String> {
    let Some(bound) = &rule.bound else {
        return Ok(());
    };
    let differs = |field: &str| match field {
        FIELD_DHT => rule.dht != changed.dht,
        FIELD_PEX => rule.pex != changed.pex,
        FIELD_LSD => rule.lsd != changed.lsd,
        FIELD_ENCRYPTION => rule.encryption != changed.encryption,
        FIELD_SSL => rule.ssl != changed.ssl,
        _ => false,
    };
    match bound.locked.iter().find(|field| differs(field)) {
        Some(field) => Err(format!(
            "{field} is locked by {}'s tracker file; unbind the rule to change it.",
            bound.name
        )),
        None => Ok(()),
    }
}

// One value a tracker file sets, as the rule holds it now and as the file
// leaves it; `None` is not set. A recommendation the user changed shows the
// user's value on both sides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub field: &'static str,
    pub locked: bool,
    pub old: Option<String>,
    pub new: Option<String>,
}

// What applying a tracker file would do, with nothing secret in it: the
// identity by its subject and expiry, the archive passwords by their count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Preview {
    pub name: String,
    pub revision: String,
    pub host_pattern: String,
    // The rule the file fills; `None` makes a new one.
    pub rule_id: Option<u32>,
    pub changes: Vec<Change>,
    pub has_ssl: bool,
    pub ssl: Option<super::model::CertificateSummary>,
    // `None` when the file says nothing of them.
    pub archive_passwords: Option<usize>,
    pub fingerprint: String,
}

impl TrackerFile {
    // What `apply` would make of `rule`, value by value.
    pub fn preview(&self, rule: Option<&TrackerRule>, fingerprint: String) -> Preview {
        let applied = self.apply(rule, 0);
        let old = rule.cloned().unwrap_or_default();
        let (required, recommended) = (&self.required, &self.recommended);
        let mut changes = Vec::new();
        let mut push = |field, locked: bool, set: bool, old: Option<String>, new| {
            if set {
                changes.push(Change {
                    field,
                    locked,
                    old,
                    new,
                });
            }
        };
        let flag = |value: Option<bool>| value.map(|value| value.to_string());
        for (field, required, recommended, old, new) in [
            (
                FIELD_DHT,
                required.dht,
                recommended.dht,
                old.dht,
                applied.dht,
            ),
            (
                FIELD_PEX,
                required.pex,
                recommended.pex,
                old.pex,
                applied.pex,
            ),
            (
                FIELD_LSD,
                required.lsd,
                recommended.lsd,
                old.lsd,
                applied.lsd,
            ),
        ] {
            let set = required.is_some() || recommended.is_some();
            push(field, required.is_some(), set, flag(old), flag(new));
        }
        let encryption = |value: Option<TorrentEncryption>| value.map(|e| e.as_str().to_owned());
        push(
            FIELD_ENCRYPTION,
            required.encryption.is_some(),
            required.encryption.is_some() || recommended.encryption.is_some(),
            encryption(old.encryption),
            encryption(applied.encryption),
        );
        if let Some(seeding) = recommended.seeding {
            // A leaf as a change shows it: absent while it inherits.
            fn text<T>(leaf: Leaf<T>, show: impl Fn(T) -> String) -> Option<String> {
                match leaf {
                    Leaf::Inherit => None,
                    Leaf::Off => Some("off".to_owned()),
                    Leaf::Set(value) => Some(show(value)),
                }
            }
            let (before, after) = (
                old.seeding.unwrap_or_default(),
                applied.seeding.unwrap_or_default(),
            );
            let number = |value: f32| value.to_string();
            let minutes = |value: u32| value.to_string();
            let mode = |mode: SeedingGoalMode| mode.as_str().to_owned();
            let action = |action: SeedingAction| action.as_str().to_owned();
            let counting = |counting: SeedTimeCounting| counting.as_str().to_owned();
            for (field, set, old, new) in [
                (
                    "seeding.ratio_goal",
                    !seeding.ratio_goal.is_inherit(),
                    text(before.ratio_goal, number),
                    text(after.ratio_goal, number),
                ),
                (
                    "seeding.seed_time_goal_minutes",
                    !seeding.seed_time_goal_minutes.is_inherit(),
                    text(before.seed_time_goal_minutes, minutes),
                    text(after.seed_time_goal_minutes, minutes),
                ),
                (
                    "seeding.inactive_goal_minutes",
                    !seeding.inactive_goal_minutes.is_inherit(),
                    text(before.inactive_goal_minutes, minutes),
                    text(after.inactive_goal_minutes, minutes),
                ),
                (
                    "seeding.mode",
                    !seeding.mode.is_inherit(),
                    text(before.mode, mode),
                    text(after.mode, mode),
                ),
                (
                    "seeding.action",
                    !seeding.action.is_inherit(),
                    text(before.action, action),
                    text(after.action, action),
                ),
                (
                    "seeding.seed_time_counting",
                    !seeding.seed_time_counting.is_inherit(),
                    text(before.seed_time_counting, counting),
                    text(after.seed_time_counting, counting),
                ),
            ] {
                push(field, false, set, old, new);
            }
        }
        Preview {
            name: self.name.clone(),
            revision: self.revision.clone(),
            host_pattern: self.host_pattern.clone(),
            rule_id: rule.map(|rule| rule.id),
            changes,
            has_ssl: self.ssl.is_some(),
            ssl: self.ssl.as_ref().and_then(TrackerCertificate::summary),
            archive_passwords: self.archive_passwords.as_ref().map(ArchivePasswords::len),
            fingerprint,
        }
    }
}

// The fingerprint of a preview: the file's bytes and the rule it fills as
// that rule stood, so an apply after either changed is refused. It is keyed
// with a secret this process draws once, so it says nothing of what the file
// holds; a restart between preview and apply makes the preview stale.
pub fn fingerprint(bytes: &[u8], rule: Option<&TrackerRule>) -> String {
    static KEY: std::sync::OnceLock<[u8; 32]> = std::sync::OnceLock::new();
    let key = KEY.get_or_init(|| {
        let mut key = [0u8; 32];
        getrandom::fill(&mut key).expect("the system's random source");
        key
    });
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
    if let Some(rule) = rule {
        // Debug names every value but the identity's key and the archive
        // passwords, which follow whole.
        hasher.update(format!("{rule:?}").as_bytes());
        if let Some(ssl) = &rule.ssl {
            hasher.update(ssl.certificate_pem.as_bytes());
            hasher.update(ssl.private_key_pem.as_bytes());
        }
        for password in rule.archive_passwords.as_slice() {
            hasher.update(&(password.len() as u64).to_le_bytes());
            hasher.update(password.as_bytes());
        }
    }
    hasher.finalize().to_hex().to_string()
}

// Reads `bytes` as a tracker file, opening an archive with `password` when
// it is one. An error is one sentence for the user; it never holds the
// password or a byte of the file beyond the key or member it names.
pub fn read(bytes: &[u8], password: Option<&str>) -> Result<TrackerFile, String> {
    if bytes.len() > MAX_UPLOAD_BYTES {
        return Err(format!(
            "The tracker file is larger than {} KiB.",
            MAX_UPLOAD_BYTES / 1024
        ));
    }
    let password = password.filter(|password| !password.is_empty());
    let Some(kind) = Kind::of(bytes) else {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| "The tracker file is neither TOML nor a 7z, ZIP or RAR archive.")?;
        return parse(text, &Members::default());
    };
    let _slot = DecodeSlot::take().ok_or_else(|| {
        format!("{MAX_DECODING} tracker files are being read already; try again in a moment.")
    })?;
    let members = match kind {
        Kind::SevenZip => sevenz(bytes, password)?,
        Kind::Zip => zip(bytes, password)?,
        Kind::Rar => rar(bytes, password)?,
    };
    let mut tomls = members
        .files
        .keys()
        .filter(|name| name.to_ascii_lowercase().ends_with(".toml"));
    let toml = match (tomls.next(), tomls.next()) {
        (Some(toml), None) => toml.clone(),
        (None, _) => return Err("The archive holds no TOML file.".to_owned()),
        (Some(_), Some(_)) => return Err("The archive holds more than one TOML file.".to_owned()),
    };
    let text = std::str::from_utf8(&members.files[&toml])
        .map_err(|_| format!("{toml} is not UTF-8 text."))?;
    let file = parse(text, &members)?;
    // Every member is the TOML or a file the TOML names.
    let named = members.named.borrow();
    if let Some(extra) = members
        .files
        .keys()
        .find(|name| **name != toml && !named.contains(name.as_str()))
    {
        return Err(format!(
            "The archive holds {extra}, which the tracker file does not name."
        ));
    }
    Ok(file)
}

#[derive(Clone, Copy)]
enum Kind {
    SevenZip,
    Zip,
    Rar,
}

impl Kind {
    fn of(bytes: &[u8]) -> Option<Self> {
        if bytes.starts_with(&[0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c]) {
            Some(Self::SevenZip)
        } else if [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"]
            .iter()
            .any(|signature| bytes.starts_with(*signature))
        {
            Some(Self::Zip)
        } else if bytes.starts_with(b"Rar!\x1a\x07") {
            Some(Self::Rar)
        } else {
            None
        }
    }
}

// One of `MAX_DECODING` places to decode an archive in; the place is given
// back when it is dropped.
struct DecodeSlot;

static DECODING: AtomicUsize = AtomicUsize::new(0);

impl DecodeSlot {
    fn take() -> Option<Self> {
        DECODING
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |taken| {
                (taken < MAX_DECODING).then_some(taken + 1)
            })
            .ok()
            .map(|_| Self)
    }
}

impl Drop for DecodeSlot {
    fn drop(&mut self) {
        DECODING.fetch_sub(1, Ordering::AcqRel);
    }
}

// An archive's files by path, and the paths the TOML names as it is read.
#[derive(Default)]
struct Members {
    files: BTreeMap<String, Vec<u8>>,
    named: std::cell::RefCell<std::collections::BTreeSet<String>>,
}

impl Members {
    // Counts one more entry, then keeps a file's bytes when it is not a
    // directory, refusing what a tracker file never holds.
    fn push(
        &mut self,
        entries: &mut usize,
        raw_name: &str,
        directory: bool,
        link: bool,
        bytes: impl FnOnce(u64) -> Result<Vec<u8>, String>,
    ) -> Result<(), String> {
        *entries += 1;
        if *entries > MAX_ENTRIES {
            return Err(format!(
                "The archive holds more than {MAX_ENTRIES} entries."
            ));
        }
        let name = member_path(raw_name)?;
        if link {
            return Err(format!("The archive's {name} is a link."));
        }
        if directory {
            return Ok(());
        }
        if is_archive_name(&name) {
            return Err(format!("The archive's {name} is an archive inside it."));
        }
        let used: u64 = self.files.values().map(|bytes| bytes.len() as u64).sum();
        let left = MAX_DECODED_BYTES.saturating_sub(used);
        let bytes = bytes(left)?;
        if bytes.len() as u64 > left {
            return Err(too_large());
        }
        if Kind::of(&bytes).is_some() {
            return Err(format!("The archive's {name} is an archive inside it."));
        }
        if self.files.insert(name.clone(), bytes).is_some() {
            return Err(format!("The archive holds {name} twice."));
        }
        Ok(())
    }

    // The bytes of the member `key` names, recording that the TOML named it.
    fn named(&self, key: &str, path: &str) -> Result<&[u8], String> {
        let name = member_path(path).map_err(|_| {
            format!("ssl.{key} must name a file in the archive, not a path outside it.")
        })?;
        let bytes = self
            .files
            .get(&name)
            .ok_or_else(|| format!("ssl.{key} names {name}, which the archive does not hold."))?;
        self.named.borrow_mut().insert(name);
        Ok(bytes)
    }
}

fn too_large() -> String {
    format!(
        "The archive decodes to more than {} KiB.",
        MAX_DECODED_BYTES / 1024
    )
}

// A member's path as `a/b`, refused when it is absolute, climbs out of the
// archive, or names a drive.
fn member_path(raw: &str) -> Result<String, String> {
    let unsafe_path = || format!("The archive's {raw} is a path outside it.");
    if raw.contains('\0') {
        return Err(unsafe_path());
    }
    let normalized = raw.replace('\\', "/");
    if normalized.starts_with('/') {
        return Err(unsafe_path());
    }
    let mut parts = Vec::new();
    for part in normalized.split('/') {
        match part {
            "" | "." => {}
            ".." => return Err(unsafe_path()),
            part if part.len() >= 2 && part.as_bytes()[1] == b':' => return Err(unsafe_path()),
            part => parts.push(part),
        }
    }
    if parts.is_empty() {
        return Err(unsafe_path());
    }
    Ok(parts.join("/"))
}

fn is_archive_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    [
        ".7z", ".zip", ".rar", ".001", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".zst",
    ]
    .iter()
    .any(|suffix| lower.ends_with(suffix))
}

// Reads `reader` to its end, refusing more than `limit` bytes.
fn read_at_most(reader: &mut dyn Read, limit: u64) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

const NEEDS_PASSWORD: &str = "The archive needs a password.";
const WRONG_PASSWORD: &str = "The password does not open the archive.";
const MULTI_VOLUME: &str = "The archive is one volume of several; a tracker file is one volume.";

fn sevenz(bytes: &[u8], password: Option<&str>) -> Result<Members, String> {
    use sevenz_turbo::{ArchiveLimits, ArchiveReader, BlockErrorKind, Error, Password};

    // A first volume names an end header past its own end.
    let declared_end = bytes
        .get(12..28)
        .map(|header| {
            let offset = u64::from_le_bytes(header[..8].try_into().unwrap());
            let size = u64::from_le_bytes(header[8..].try_into().unwrap());
            32u64.saturating_add(offset).saturating_add(size)
        })
        .unwrap_or(0);
    if declared_end > bytes.len() as u64 {
        return Err(MULTI_VOLUME.to_owned());
    }
    let limits = ArchiveLimits {
        memory_limit_bytes: MAX_DICTIONARY_BYTES,
        max_end_header_bytes: MAX_HEADER_BYTES,
        max_header_unpacked_bytes: MAX_HEADER_BYTES,
        max_entries: MAX_ENTRIES as u64,
        max_unpack_bytes: MAX_DECODED_BYTES,
        max_aes_cycles_power: MAX_7Z_AES_CYCLES_POWER,
        max_aes_kdf_rounds: MAX_7Z_KDF_ROUNDS,
        reject_unsafe_paths: true,
        ..ArchiveLimits::default()
    };
    let refusal = |error: Error| -> String {
        let password_failure = |error: &Error| {
            matches!(
                error,
                Error::PasswordRequired
                    | Error::MaybeBadPassword(_)
                    | Error::ChecksumVerificationFailed
                    | Error::NextHeaderCrcMismatch
                    | Error::Corrupted(_)
                    | Error::BlockDecode {
                        kind: BlockErrorKind::Password
                            | BlockErrorKind::ChecksumMismatch
                            | BlockErrorKind::Corrupted,
                        ..
                    }
            )
        };
        match &error {
            Error::UnsafeEntryName { name, .. } => {
                format!("The archive's {name} is a path outside it.")
            }
            Error::LimitExceeded { .. } | Error::MemoryLimited { .. } => {
                format!("The archive asks for more than a tracker file may: {error}.")
            }
            Error::EndHeaderTooLarge { .. } => "The archive's headers are too large.".to_owned(),
            Error::PasswordRequired if password.is_none() => NEEDS_PASSWORD.to_owned(),
            error if password.is_some() && password_failure(error) => WRONG_PASSWORD.to_owned(),
            error if password_failure(error) => NEEDS_PASSWORD.to_owned(),
            error => format!("The archive cannot be read: {error}."),
        }
    };
    let password_given = password.is_some();
    let password = password.map_or_else(Password::empty, Password::from);
    let mut reader =
        ArchiveReader::with_limits(Cursor::new(bytes), password, limits).map_err(refusal)?;
    let mut members = Members::default();
    let mut entries = 0;
    let mut refused = None;
    let walked = reader.for_each_entries(|entry, reader| {
        if entry.is_anti_item() {
            return Ok(true);
        }
        let pushed = members.push(
            &mut entries,
            entry.name(),
            entry.is_directory(),
            entry.is_symlink(),
            |left| {
                read_at_most(reader, left).map_err(|error| {
                    // Enciphered content that does not decode under the
                    // password given is the password's fault.
                    let locked = error
                        .get_ref()
                        .and_then(|inner| inner.downcast_ref::<Error>())
                        .is_some_and(|inner| matches!(inner, Error::PasswordRequired));
                    match password_given {
                        true => WRONG_PASSWORD.to_owned(),
                        false if locked => NEEDS_PASSWORD.to_owned(),
                        false => format!("The archive's {} cannot be read: {error}.", entry.name()),
                    }
                })
            },
        );
        match pushed {
            Ok(()) => Ok(true),
            Err(refusal) => {
                refused = Some(refusal);
                Ok(false)
            }
        }
    });
    if let Some(refusal) = refused {
        return Err(refusal);
    }
    walked.map_err(refusal)?;
    Ok(members)
}

fn zip(bytes: &[u8], password: Option<&str>) -> Result<Members, String> {
    use ::zip::result::ZipError;

    // A split archive's first part opens with the spanning marker.
    if bytes.starts_with(b"PK\x07\x08") {
        return Err(MULTI_VOLUME.to_owned());
    }
    let mut archive = ::zip::ZipArchive::new(Cursor::new(bytes)).map_err(|error| match error {
        ZipError::UnsupportedArchive(why) if why.contains("multi-disk") => MULTI_VOLUME.to_owned(),
        error => format!("The archive cannot be read: {error}."),
    })?;
    if archive.len() > MAX_ENTRIES {
        return Err(format!(
            "The archive holds more than {MAX_ENTRIES} entries."
        ));
    }
    let mut members = Members::default();
    let mut entries = 0;
    for index in 0..archive.len() {
        let entry = match password {
            Some(password) => archive.by_index_decrypt(index, password.as_bytes()),
            None => archive.by_index(index),
        };
        let mut entry = entry.map_err(|error| match error {
            ZipError::InvalidPassword => WRONG_PASSWORD.to_owned(),
            ZipError::UnsupportedArchive(why) if why == ZipError::PASSWORD_REQUIRED => {
                NEEDS_PASSWORD.to_owned()
            }
            error => format!("The archive cannot be read: {error}."),
        })?;
        let name = entry
            .name()
            .map_err(|error| format!("The archive cannot be read: {error}."))?
            .into_owned();
        let (directory, link, encrypted) = (entry.is_dir(), entry.is_symlink(), entry.encrypted());
        members.push(&mut entries, &name, directory, link, |left| {
            read_at_most(&mut entry, left).map_err(|error| {
                if encrypted && error.kind() == std::io::ErrorKind::InvalidData {
                    WRONG_PASSWORD.to_owned()
                } else {
                    format!("The archive's {name} cannot be read: {error}.")
                }
            })
        })?;
    }
    Ok(members)
}

fn rar(bytes: &[u8], password: Option<&str>) -> Result<Members, String> {
    use unrar_rs::RarError;

    let password_failure = |error: &RarError| {
        matches!(
            error,
            RarError::EncryptedArchive
                | RarError::EncryptedMember { .. }
                | RarError::InvalidPassword
                | RarError::WrongPassword { .. }
        )
    };
    let refusal = |error: RarError, encrypted: bool| -> String {
        match &error {
            RarError::ResourceLimit { .. } => {
                format!("The archive asks for more than a tracker file may: {error}.")
            }
            RarError::MissingVolume { .. } => MULTI_VOLUME.to_owned(),
            error if password_failure(error) && password.is_none() => NEEDS_PASSWORD.to_owned(),
            error if password_failure(error) => WRONG_PASSWORD.to_owned(),
            // RAR4 checks no password: a wrong one shows as content that
            // does not match its checksum.
            RarError::DataCrcMismatch { .. } if encrypted && password.is_some() => {
                WRONG_PASSWORD.to_owned()
            }
            RarError::DataCrcMismatch { .. } if encrypted => NEEDS_PASSWORD.to_owned(),
            error => format!("The archive cannot be read: {error}."),
        }
    };
    let source = Cursor::new(bytes.to_vec());
    let mut archive = match password {
        Some(password) => unrar_rs::RarArchive::open_with_password(source, password),
        None => unrar_rs::RarArchive::open(source),
    }
    .map_err(|error| refusal(error, true))?;
    archive.set_limits(unrar_rs::Limits {
        max_header_size: MAX_HEADER_BYTES,
        max_data_segment: MAX_DECODED_BYTES,
        max_unpacked_size: MAX_DECODED_BYTES,
        max_dict_size: MAX_DICTIONARY_BYTES,
        max_ppmd_arena_size: MAX_DICTIONARY_BYTES,
    });
    if archive.more_volumes() {
        return Err(MULTI_VOLUME.to_owned());
    }
    let metadata = archive.metadata();
    if metadata.members.len() > MAX_ENTRIES {
        return Err(format!(
            "The archive holds more than {MAX_ENTRIES} entries."
        ));
    }
    let mut members = Members::default();
    let mut entries = 0;
    for (index, member) in metadata.members.iter().enumerate() {
        if !archive.missing_volumes(&member.name).is_empty() {
            return Err(MULTI_VOLUME.to_owned());
        }
        let link = member.is_symlink || member.is_hardlink || member.is_file_copy;
        let encrypted = member.is_encrypted;
        members.push(
            &mut entries,
            &member.raw_name,
            member.is_directory,
            link,
            |left| {
                if member.unpacked_size.is_some_and(|size| size > left) {
                    return Err(too_large());
                }
                let mut entry = archive
                    .by_index(index)
                    .map_err(|error| refusal(error, encrypted))?;
                read_at_most(&mut entry, left)
                    .map_err(|error| refusal(RarError::from(error), encrypted))
            },
        )?;
    }
    Ok(members)
}

// The sections and keys a tracker file may hold.
const TOP_KEYS: [&str; 8] = [
    "version",
    "name",
    "revision",
    "host_pattern",
    "required",
    "recommended",
    "archive_passwords",
    "ssl",
];
const SEEDING_KEYS: [&str; 6] = [
    "ratio_goal",
    "seed_time_goal_minutes",
    "inactive_goal_minutes",
    "mode",
    "action",
    "seed_time_counting",
];

fn parse(text: &str, members: &Members) -> Result<TrackerFile, String> {
    let table: toml::Table = text.parse().map_err(|error: toml::de::Error| {
        format!("The tracker file is not TOML: {}", error.message())
    })?;
    if let Some(key) = table.keys().find(|key| !TOP_KEYS.contains(&key.as_str())) {
        return Err(not_allowed(key));
    }
    match table.get("version") {
        Some(toml::Value::Integer(1)) => {}
        Some(_) => return Err("The tracker file's version must be 1.".to_owned()),
        None => return Err("The tracker file has no version.".to_owned()),
    }
    let name = text_field(&table, "name", MAX_NAME_BYTES)?;
    let revision = text_field(&table, "revision", MAX_REVISION_BYTES)?;
    let host_pattern = match table.get("host_pattern") {
        Some(toml::Value::String(pattern)) => TrackerRule::pattern(pattern)
            .map_err(|error| format!("host_pattern is refused: {error}."))?,
        Some(_) => return Err("host_pattern must be text.".to_owned()),
        None => return Err("The tracker file has no host_pattern.".to_owned()),
    };
    let required = terms(&table, "required")?;
    let recommended = terms(&table, "recommended")?;
    if let Some(field) = required
        .fields()
        .into_iter()
        .find(|field| recommended.fields().contains(field))
    {
        return Err(format!("{field} is in both [required] and [recommended]."));
    }
    Ok(TrackerFile {
        name,
        revision,
        host_pattern,
        required,
        recommended,
        archive_passwords: archive_passwords(&table)?,
        ssl: ssl(&table, members)?,
    })
}

fn not_allowed(key: &str) -> String {
    format!("{key} is not something a tracker file may set.")
}

fn section<'t>(table: &'t toml::Table, key: &str) -> Result<Option<&'t toml::Table>, String> {
    match table.get(key) {
        None => Ok(None),
        Some(toml::Value::Table(section)) => Ok(Some(section)),
        Some(_) => Err(format!("{key} must be a [{key}] section.")),
    }
}

fn text_field(table: &toml::Table, key: &str, max: usize) -> Result<String, String> {
    match table.get(key) {
        Some(toml::Value::String(value)) => {
            let value = value.trim();
            if value.is_empty() {
                Err(format!("The tracker file's {key} is empty."))
            } else if value.len() > max {
                Err(format!(
                    "The tracker file's {key} is longer than {max} bytes."
                ))
            } else {
                Ok(value.to_owned())
            }
        }
        Some(_) => Err(format!("{key} must be text.")),
        None => Err(format!("The tracker file has no {key}.")),
    }
}

fn terms(table: &toml::Table, section_name: &str) -> Result<Terms, String> {
    let mut terms = Terms::default();
    let Some(section) = section(table, section_name)? else {
        return Ok(terms);
    };
    let required = section_name == "required";
    for (key, value) in section {
        let path = format!("{section_name}.{key}");
        let switch = || match value {
            toml::Value::Boolean(on) => Ok(Some(*on)),
            _ => Err(format!("{path} must be true or false.")),
        };
        match key.as_str() {
            FIELD_DHT => terms.dht = switch()?,
            FIELD_PEX => terms.pex = switch()?,
            FIELD_LSD => terms.lsd = switch()?,
            FIELD_ENCRYPTION => match value {
                toml::Value::String(mode) if mode == TorrentEncryption::Require.as_str() => {
                    terms.encryption = Some(TorrentEncryption::Require);
                }
                toml::Value::String(mode) => {
                    return Err(format!(
                        "{path} may only be \"require\", not \"{mode}\"; a tracker may only tighten it."
                    ));
                }
                _ => return Err(format!("{path} must be \"require\".")),
            },
            "seeding" if required => {
                return Err(format!(
                    "{path} is refused: a seeding goal may be recommended, never required."
                ));
            }
            "seeding" => terms.seeding = Some(seeding(value, &path)?),
            _ => return Err(not_allowed(&path)),
        }
    }
    Ok(terms)
}

fn seeding(value: &toml::Value, path: &str) -> Result<SeedingTerms, String> {
    let toml::Value::Table(table) = value else {
        return Err(format!("{path} must be a table."));
    };
    if let Some(key) = table
        .keys()
        .find(|key| !SEEDING_KEYS.contains(&key.as_str()))
    {
        return Err(not_allowed(&format!("{path}.{key}")));
    }
    let minutes = |key: &str| -> Result<Option<u32>, String> {
        match table.get(key) {
            None => Ok(None),
            Some(toml::Value::Integer(minutes)) => u32::try_from(*minutes)
                .map(Some)
                .map_err(|_| format!("{path}.{key} must be a whole number of minutes.")),
            Some(_) => Err(format!("{path}.{key} must be a whole number of minutes.")),
        }
    };
    let ratio_goal = match table.get("ratio_goal") {
        None => None,
        Some(value) => {
            let ratio = match value {
                toml::Value::Float(ratio) => *ratio,
                toml::Value::Integer(ratio) => *ratio as f64,
                _ => f64::NAN,
            };
            if !ratio.is_finite() || ratio < 0.0 || ratio > f64::from(f32::MAX) {
                return Err(format!("{path}.ratio_goal must be a number, zero or more."));
            }
            Some(ratio as f32)
        }
    };
    let word = |key: &str| -> Result<Option<&str>, String> {
        match table.get(key) {
            None => Ok(None),
            Some(toml::Value::String(word)) => Ok(Some(word)),
            Some(_) => Err(format!("{path}.{key} must be text.")),
        }
    };
    let mode = word("mode")?
        .map(|mode| {
            SeedingGoalMode::parse(mode)
                .ok_or_else(|| format!("{path}.mode must be \"any\" or \"all\", not \"{mode}\"."))
        })
        .transpose()?;
    let action = word("action")?
        .map(|action| {
            SeedingAction::parse(action).ok_or_else(|| {
                format!(
                    "{path}.action must be \"stop\", \"remove\" or \"remove_with_data\", not \"{action}\"."
                )
            })
        })
        .transpose()?;
    let counting = word("seed_time_counting")?
        .map(|counting| {
            SeedTimeCounting::parse(counting).ok_or_else(|| {
                format!(
                    "{path}.seed_time_counting must be \"cumulative\" or \"consecutive\", not \"{counting}\"."
                )
            })
        })
        .transpose()?;
    Ok(SeedingTerms {
        ratio_goal: Leaf::of(ratio_goal),
        seed_time_goal_minutes: Leaf::of(minutes("seed_time_goal_minutes")?),
        inactive_goal_minutes: Leaf::of(minutes("inactive_goal_minutes")?),
        mode: Leaf::of(mode),
        action: Leaf::of(action),
        seed_time_counting: Leaf::of(counting),
    })
}

// The archive passwords in the order given, blanks and repeats dropped, each
// kept exactly as written.
fn archive_passwords(table: &toml::Table) -> Result<Option<ArchivePasswords>, String> {
    let Some(section) = section(table, "archive_passwords")? else {
        return Ok(None);
    };
    if let Some(key) = section.keys().find(|key| *key != "values") {
        return Err(not_allowed(&format!("archive_passwords.{key}")));
    }
    let Some(values) = section.get("values") else {
        return Err("[archive_passwords] has no values.".to_owned());
    };
    let toml::Value::Array(values) = values else {
        return Err("archive_passwords.values must be a list of text.".to_owned());
    };
    let passwords = values
        .iter()
        .map(|value| match value {
            toml::Value::String(password) => Ok(password.clone()),
            _ => Err("archive_passwords.values must be a list of text.".to_owned()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    ArchivePasswords::new(passwords)
        .map(Some)
        .map_err(|refusal| format!("archive_passwords.values {refusal}"))
}

// The identity the tracker issued, inline or as two members of the archive,
// checked as the rule editor checks one.
fn ssl(table: &toml::Table, members: &Members) -> Result<Option<TrackerCertificate>, String> {
    const KEYS: [&str; 4] = [
        "certificate_pem",
        "private_key_pem",
        "certificate_file",
        "private_key_file",
    ];
    let Some(section) = section(table, "ssl")? else {
        return Ok(None);
    };
    if let Some(key) = section.keys().find(|key| !KEYS.contains(&key.as_str())) {
        return Err(not_allowed(&format!("ssl.{key}")));
    }
    let text = |key: &str| -> Result<Option<&str>, String> {
        match section.get(key) {
            None => Ok(None),
            Some(toml::Value::String(value)) => Ok(Some(value)),
            Some(_) => Err(format!("ssl.{key} must be text.")),
        }
    };
    let inline = (text("certificate_pem")?, text("private_key_pem")?);
    let files = (text("certificate_file")?, text("private_key_file")?);
    let certificate = match (inline, files) {
        ((Some(certificate), Some(key)), (None, None)) => TrackerCertificate {
            certificate_pem: certificate.to_owned(),
            private_key_pem: key.to_owned(),
        },
        ((None, None), (Some(certificate), Some(key))) => {
            if members.files.is_empty() {
                return Err(
                    "[ssl] names archive files, and the tracker file is not in an archive."
                        .to_owned(),
                );
            }
            let pem = |key: &str, path: &str| -> Result<String, String> {
                String::from_utf8(members.named(key, path)?.to_vec())
                    .map_err(|_| format!("ssl.{key} names a file that is not PEM text."))
            };
            TrackerCertificate {
                certificate_pem: pem("certificate_file", certificate)?,
                private_key_pem: pem("private_key_file", key)?,
            }
        }
        ((None, None), (None, None)) => return Err("[ssl] is empty.".to_owned()),
        ((Some(_) | None, Some(_) | None), (None, None)) | ((None, None), _) => {
            return Err("[ssl] needs both the certificate and the private key.".to_owned());
        }
        _ => {
            return Err(
                "[ssl] gives the identity inline and as archive files; it may do only one."
                    .to_owned(),
            );
        }
    };
    certificate
        .identity()
        .map_err(|error| format!("The [ssl] certificate is refused: {error}"))?;
    Ok(Some(certificate))
}

#[cfg(test)]
mod tests;
