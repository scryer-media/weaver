// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One run of the RAR engine. unrar-rs walks the set's headers a file header
// at a time, volume after volume, and each member is decoded as soon as its
// header is read, straight into the sink. Reads wait for bytes still on
// their way, so the run keeps pace with a download without ever reading
// ahead of it, and a solid set keeps its dictionary across members.
//
// The members of a set that is not solid do not depend on one another, so
// the files of a volume whose bytes are all in are decoded side by side on
// the pool the budget lends, as many at once as it grants threads and as
// much as it grants memory, and said in archive order as if one by one.

use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use unrar_rs::{
    ArchiveFormat, CachedArchiveHeaders, CompressionMethod, KdfCache, RarArchive, RarError,
    ReadSeek, VolumeProvider, VolumeProviderError,
};
use weaver_graph::{
    Budget, ByteRange, Emit, Extraction, MemberId, Provenance, Sink, VolumeIndex, Volumes, Why,
};

// One member's slot when members decode side by side: empty until its
// worker is done, then its bytes or the error that stopped it.
type Decoded = Mutex<Option<Result<Vec<u8>, RarError>>>;

pub(super) struct Run {
    password: Option<String>,
}

impl Run {
    pub(super) fn new(password: Option<String>) -> Self {
        Self { password }
    }
}

impl Extraction for Run {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>, budget: Budget) {
        let mut walk = Walk {
            input: Input {
                volumes,
                failed: Arc::new(AtomicBool::new(false)),
                reached: Mutex::default(),
            },
            sink,
            budget,
            kdf: Arc::new(KdfCache::new()),
            next: 0,
            open: None,
            reading: 0,
        };
        let Err(Stop::Say(last)) = walk.archive(self.password.as_deref()) else {
            return;
        };
        // A member left open was built on bytes the run cannot vouch for.
        if let Some(open) = walk.open.take()
            && !(walk.sink)(Emit::Retract(open.member))
        {
            return;
        }
        for emit in last {
            if !(walk.sink)(emit) {
                return;
            }
        }
    }
}

// How a run ends early: silently, because the owner stopped it or failed a
// read and so already knows why, or saying why.
enum Stop {
    Silent,
    Say(Vec<Emit>),
}

struct Open {
    member: MemberId,
    encrypted: bool,
    // The volumes the member's data spans.
    first: usize,
    last: usize,
}

struct Walk<'s, 'a> {
    input: Input,
    sink: &'s mut Sink<'a>,
    budget: Budget,
    // The keys the workers derive for encrypted members, shared so each is
    // derived once for the run.
    kdf: Arc<KdfCache>,
    // The id the next member announced gets: members are numbered in
    // archive order, so every run of the same set numbers them alike.
    next: u32,
    open: Option<Open>,
    // The volume whose headers are being read.
    reading: usize,
}

impl Walk<'_, '_> {
    fn archive(&mut self, password: Option<&str>) -> Result<(), Stop> {
        let first = self.input.index(0).map_err(|_| Stop::Silent)?;
        let Some(first) = first else {
            return Ok(());
        };
        let mut archive =
            RarArchive::open_prefix(self.input.reader(first), password, NonZeroUsize::MIN)
                .map_err(|error| self.fail(error))?;
        // Bytes may catch up with a large member as it decodes; adaptive
        // decoding picks inline or batched work from what it measures.
        archive.set_decode_mode(unrar_rs::DecodeMode::Adaptive);
        archive.set_verify(true);
        let side_by_side = self.budget.threads() > 1 && !archive.is_solid();
        let mut index = 0;
        let mut volume = 0;
        while let Some(at) = self.input.index(volume).map_err(|_| Stop::Silent)? {
            self.reading = volume;
            // A closed volume's headers are read as many ahead as members
            // can decode at once; a growing one's one at a time, so the walk
            // never waits on a header for a member it could decode now.
            let step = match side_by_side && self.input.volumes.len(at).is_some() {
                true => NonZeroUsize::new(self.budget.threads()).unwrap_or(NonZeroUsize::MIN),
                false => NonZeroUsize::MIN,
            };
            let mut headers = step;
            loop {
                let found = archive
                    .extend_volume_prefix(volume, Box::new(self.input.reader(at)), headers)
                    .map_err(|error| self.fail(error))?;
                let more = found == headers.get();
                while index < archive.len() {
                    let batch = match side_by_side {
                        true => self.batch(&archive, index, password.is_some()),
                        false => 0,
                    };
                    if batch > 1 {
                        self.side_by_side(&archive, index..index + batch, password)?;
                        index += batch;
                        continue;
                    }
                    // The last header read waits for the next ones while
                    // its volume has more, so it can decode beside them.
                    if step.get() > 1 && more && index + 1 == archive.len() {
                        break;
                    }
                    self.member(&mut archive, index, password.is_some())?;
                    index += 1;
                }
                if !more {
                    break;
                }
                headers = headers
                    .checked_add(step.get())
                    .ok_or(Stop::Say(vec![Emit::Refuse(Why::Unsupported)]))?;
            }
            volume += 1;
        }
        Ok(())
    }

    // How many members from `index` on can decode side by side: files whose
    // data lies whole in one volume whose bytes are all in, so a worker on
    // the lent pool never waits on a download, which a later header of the
    // same volume shows is not continued in the next, whose length is known,
    // and which fit in the memory the budget grants together. Each holds its
    // bytes, decoded whole before it is said, and its decoder's window, as
    // large as the dictionary its header names.
    fn batch(&self, archive: &RarArchive, index: usize, has_password: bool) -> usize {
        let mut held = 0u64;
        let mut count = 0;
        for at in index..archive.len() {
            let (Some(info), Some(after)) = (archive.member_info(at), archive.member_info(at + 1))
            else {
                break;
            };
            let volume = info.volumes.first_volume;
            let settled = u32::try_from(volume)
                .ok()
                .is_some_and(|volume| self.input.volumes.settled(VolumeIndex(volume)));
            let plain = !(info.is_symlink
                || info.is_hardlink
                || info.is_file_copy
                || info.is_directory
                || (info.is_encrypted && !has_password));
            let whole = info.volumes.last_volume == volume && after.volumes.first_volume == volume;
            let Some(length) = info.unpacked_size.filter(|_| plain && whole && settled) else {
                break;
            };
            let window = match info.compression.method {
                CompressionMethod::Store => 0,
                _ => info.compression.dict_size,
            };
            match held
                .checked_add(length)
                .and_then(|held| held.checked_add(window))
            {
                Some(total)
                    if self
                        .budget
                        .memory_bytes
                        .is_none_or(|memory| total <= memory) =>
                {
                    held = total
                }
                _ => break,
            }
            count += 1;
        }
        count
    }

    // Decode the members of `batch` on as many of the lent pool's threads
    // as the budget grants, each worker over an archive of its own made from
    // the headers the walk parsed, and say them in archive order once each
    // is whole. A member that fails is said as far as its announcement and
    // fails the run as it would decoding alone; the members after it are
    // not said.
    fn side_by_side(
        &mut self,
        archive: &RarArchive,
        batch: std::ops::Range<usize>,
        password: Option<&str>,
    ) -> Result<(), Stop> {
        let lengths: Vec<u64> = batch
            .clone()
            .map(|at| {
                archive
                    .member_info(at)
                    .and_then(|info| info.unpacked_size)
                    .unwrap_or(0)
            })
            .collect();
        let decoded: Vec<Decoded> = (0..lengths.len()).map(|_| Mutex::new(None)).collect();
        let workers = self.budget.threads().min(lengths.len());
        // The walk's headers, read once, one copy for each worker.
        let headers: Mutex<Vec<CachedArchiveHeaders>> =
            Mutex::new((0..workers).map(|_| archive.export_headers()).collect());
        let taken = AtomicUsize::new(0);
        let halted = AtomicBool::new(false);
        let (input, kdf, start) = (&self.input, &self.kdf, batch.start);
        let work = || {
            let Some(headers) = headers.lock().unwrap_or_else(PoisonError::into_inner).pop() else {
                return;
            };
            let mut own = RarArchive::from_cached_headers_with_password_and_shared_kdf_cache(
                headers,
                password.map(str::to_owned),
                Arc::clone(kdf),
            );
            own.set_decode_mode(unrar_rs::DecodeMode::Adaptive);
            own.set_verify(true);
            // Members are taken in archive order, so every one not taken
            // follows one that failed.
            while !halted.load(Ordering::Acquire) {
                let at = taken.fetch_add(1, Ordering::AcqRel);
                let Some(&length) = lengths.get(at) else {
                    break;
                };
                let mut out = Vec::with_capacity(usize::try_from(length).unwrap_or_default());
                let result = own
                    .by_index_via(start + at, input)
                    .and_then(|entry| entry.copy_to(&mut out))
                    .map(|_| out);
                if result.is_err() {
                    halted.store(true, Ordering::Release);
                }
                *decoded[at].lock().unwrap_or_else(PoisonError::into_inner) = Some(result);
            }
        };
        self.budget.pool().spread(workers, &work);
        let decoded = decoded
            .into_iter()
            .map(|slot| slot.into_inner().unwrap_or_else(PoisonError::into_inner));
        for (at, result) in batch.zip(decoded) {
            let Some(info) = archive.member_info(at) else {
                return Err(self.fail(RarError::MemberIndexOutOfRange {
                    index: at,
                    len: archive.len(),
                }));
            };
            let member = self.announce(&info)?;
            match result {
                Some(Ok(bytes)) => {
                    self.deliver(member, bytes)?;
                    self.open = None;
                    self.checksum_of(archive, at, member)?;
                    self.say(Emit::MemberClosed(member))?;
                }
                Some(Err(error)) => return Err(self.fail(error)),
                None => unreachable!("a member not taken follows one that failed"),
            }
        }
        Ok(())
    }

    // A file member's announcement, leaving it open. It states no checksum:
    // until the member's final part's header has been read, the archive
    // knows none for it. `checksum_of` states it once the member is decoded.
    fn announce(&mut self, info: &unrar_rs::MemberInfo) -> Result<MemberId, Stop> {
        let member = MemberId(self.next);
        self.next += 1;
        self.say(Emit::Member {
            member,
            name: unrar_rs::sanitize_path(&info.raw_name),
            length: info.unpacked_size,
        })?;
        self.open = Some(Open {
            member,
            encrypted: info.is_encrypted,
            first: info.volumes.first_volume,
            last: info.volumes.last_volume,
        });
        Ok(member)
    }

    // The CRC32 a file member's final part's header states for it, said
    // once the member is decoded, before it closes. Decoding reads that
    // header, so the archive is asked again then rather than at the
    // announcement. A member whose header states no CRC32, such as one
    // carrying only a BLAKE2sp hash, states nothing. Nor does an encrypted
    // RAR5 member: its header's CRC32 is keyed, and a keyed checksum is not
    // the member's own.
    fn checksum_of(
        &mut self,
        archive: &RarArchive,
        index: usize,
        member: MemberId,
    ) -> Result<(), Stop> {
        let keyed = archive.format() == ArchiveFormat::Rar5 && archive.member_is_encrypted(index);
        match archive.member_info(index).and_then(|info| info.crc32) {
            Some(crc32) if !keyed => self.say(Emit::Checksum { member, crc32 }),
            _ => Ok(()),
        }
    }

    // A member decoded whole, said in pieces no larger than a decoder's.
    fn deliver(&mut self, member: MemberId, bytes: Vec<u8>) -> Result<(), Stop> {
        const PIECE: usize = 1 << 20;
        let bytes = Bytes::from(bytes);
        let mut offset = 0;
        while offset < bytes.len() {
            let end = bytes.len().min(offset + PIECE);
            self.say(Emit::Bytes {
                member,
                offset: offset as u64,
                bytes: bytes.slice(offset..end),
                provenance: Provenance::Fresh,
            })?;
            offset = end;
        }
        Ok(())
    }

    fn member(
        &mut self,
        archive: &mut RarArchive,
        index: usize,
        has_password: bool,
    ) -> Result<(), Stop> {
        let Some(info) = archive.member_info(index) else {
            return Err(self.fail(RarError::MemberIndexOutOfRange {
                index,
                len: archive.len(),
            }));
        };
        // A link is not a file body, so it is no member and the files around
        // it stand.
        if info.is_symlink || info.is_hardlink || info.is_file_copy {
            return Ok(());
        }
        // A directory is a member with nothing in it, named with a trailing
        // slash as the store names it.
        if info.is_directory {
            let mut name = unrar_rs::sanitize_path(&info.raw_name);
            if !name.ends_with('/') {
                name.push('/');
            }
            let member = MemberId(self.next);
            self.next += 1;
            self.say(Emit::Member {
                member,
                name,
                length: Some(0),
            })?;
            return self.say(Emit::MemberClosed(member));
        }
        if info.is_encrypted && !has_password {
            return Err(Stop::Say(vec![Emit::NeedsPassword]));
        }
        let member = self.announce(&info)?;
        let mut out = Out {
            sink: &mut *self.sink,
            member,
            offset: 0,
            stopped: false,
        };
        let decoded = archive
            .by_index_via(index, &self.input)
            .and_then(|entry| entry.copy_to(&mut out));
        match decoded {
            Ok(_) => {
                self.open = None;
                self.checksum_of(archive, index, member)?;
                self.say(Emit::MemberClosed(member))
            }
            Err(_) if out.stopped => Err(Stop::Silent),
            Err(error) => Err(self.fail(error)),
        }
    }

    fn say(&mut self, emit: Emit) -> Result<(), Stop> {
        if (self.sink)(emit) {
            Ok(())
        } else {
            Err(Stop::Silent)
        }
    }

    fn fail(&self, error: RarError) -> Stop {
        if self.input.failed.load(Ordering::Acquire) {
            return Stop::Silent;
        }
        let encrypted = self.open.as_ref().is_some_and(|open| open.encrypted);
        let (first, last) = self
            .open
            .as_ref()
            .map_or((self.reading, self.reading), |open| (open.first, open.last));
        Stop::Say(verdict(&error, encrypted, first..=last, &self.input))
    }
}

// What a RAR error means for the job, by its class. Damage is said on the
// volumes the failing member spans, or the one whose headers failed, so a
// repair can bring their bytes back. A run reads verified bytes only, and a
// read that failed ends it silently, so a failure that gets here is on bytes
// as they were posted: on an encrypted member, a wrong key decrypts to bytes
// the decoder or the checksum rejects, and the failure asks for another
// password before it is called damage.
fn verdict(
    error: &RarError,
    encrypted: bool,
    spans: std::ops::RangeInclusive<usize>,
    input: &Input,
) -> Vec<Emit> {
    match rar_error_class(error) {
        Class::PasswordRequired => vec![Emit::NeedsPassword],
        Class::Recoverable if encrypted => vec![Emit::NeedsPassword],
        Class::Recoverable => {
            // A closed volume is damaged whole; a growing one as far as the
            // run read it, for that is all the run can say of it. A volume
            // the run never read says nothing.
            spans
                .filter_map(|position| {
                    let volume = input.index(position).ok()??;
                    let known_end = input
                        .volumes
                        .len(volume)
                        .unwrap_or_else(|| input.reached(volume));
                    (known_end > 0).then(|| Emit::Damaged {
                        volume,
                        range: ByteRange::new(0, known_end),
                    })
                })
                .collect()
        }
        Class::Terminal => vec![terminal(error, encrypted)],
    }
}

fn terminal(error: &RarError, encrypted: bool) -> Emit {
    match error {
        // A wrong key's bytes, as the decoder rejects them.
        RarError::Blake2Mismatch { .. }
        | RarError::CorruptArchive { .. }
        | RarError::InvalidHuffmanTable
            if encrypted =>
        {
            Emit::NeedsPassword
        }
        RarError::UnsafeLinkTarget { .. } | RarError::UnsupportedLinkType { .. } => {
            Emit::Refuse(Why::NotAFile)
        }
        RarError::UnsupportedFormat { .. }
        | RarError::UnsupportedCompression { .. }
        | RarError::UnsupportedEncryption { .. }
        | RarError::UnsupportedEncryptionKdf { .. }
        | RarError::UnsupportedFilter { .. }
        | RarError::DictionaryTooLarge { .. }
        | RarError::ResourceLimit { .. }
        | RarError::VolumeProviderRequired { .. }
        | RarError::SolidOrderViolation { .. } => Emit::Refuse(Why::Unsupported),
        _ => Emit::Refuse(Why::Unrepairable),
    }
}

enum Class {
    Recoverable,
    PasswordRequired,
    Terminal,
}

fn rar_io_error_class(error: &io::Error) -> Class {
    if let Some(inner) = error.get_ref()
        && let Some(error) = inner.downcast_ref::<RarError>()
    {
        return rar_error_class(error);
    }
    unrar_0_11_erased_class(&error.to_string())
        .or_else(|| untyped_io_class(error))
        .unwrap_or(Class::Terminal)
}

fn rar_error_class(error: &RarError) -> Class {
    match error {
        RarError::Io(error) => rar_io_error_class(error),
        RarError::InvalidSignature
        | RarError::TruncatedHeader { .. }
        | RarError::HeaderCrcMismatch { .. }
        | RarError::DataCrcMismatch { .. }
        | RarError::PackedDataCrcMismatch { .. } => Class::Recoverable,
        RarError::CorruptArchive { detail }
            if unrar_0_11_block_header_checksum_mismatch(detail) =>
        {
            Class::Recoverable
        }
        error if rar_error_is_password_related(error) => Class::PasswordRequired,
        _ => Class::Terminal,
    }
}

fn rar_error_is_password_related(error: &RarError) -> bool {
    matches!(
        error,
        RarError::EncryptedArchive
            | RarError::EncryptedMember { .. }
            | RarError::InvalidPassword
            | RarError::WrongPassword { .. }
    )
}

// unrar-rs 0.11 renders a header or signature error on a continuation
// volume into text; the renderings of the damage errors are read back.
fn unrar_0_11_erased_class(text: &str) -> Option<Class> {
    let lower = text.to_ascii_lowercase();
    let damaged = lower.contains("checksum")
        || lower.contains("crc mismatch")
        || lower.contains("truncated header at offset")
        || lower.contains("not a rar archive (bad signature)");
    damaged.then_some(Class::Recoverable)
}

fn untyped_io_class(error: &io::Error) -> Option<Class> {
    let text = error.to_string();
    std_read_exact_eof(&text).then_some(Class::Recoverable)
}

// std: `Read::read_exact` past the end of its reader.
fn std_read_exact_eof(text: &str) -> bool {
    text.contains("failed to fill whole buffer")
}

fn unrar_0_11_block_header_checksum_mismatch(detail: &str) -> bool {
    detail.starts_with("RAR5 block header checksum mismatch")
}

// The owner's volumes as unrar-rs reads them: one reader per open, each with
// its own cursor. A read the owner fails is remembered, so the error unrar-rs
// makes of it ends the run silently instead of being taken for damage. Each
// volume keeps the furthest byte any of its readers reached, across opens.
struct Input {
    volumes: Arc<dyn Volumes>,
    failed: Arc<AtomicBool>,
    reached: Mutex<BTreeMap<VolumeIndex, Arc<AtomicU64>>>,
}

impl Input {
    // The volume unrar-rs numbers `position`: bound volume `position`, for
    // a RAR set's volumes are numbered by their names. A position short of
    // the last bound volume with nothing bound there is still that volume:
    // reading it says it is lost, which is what the set is missing. Past
    // the last bound volume there is none.
    fn index(&self, position: usize) -> io::Result<Option<VolumeIndex>> {
        let Ok(position) = u32::try_from(position) else {
            return Ok(None);
        };
        let after = position.checked_sub(1).map(VolumeIndex);
        match self.volumes.next(after) {
            Ok(found) => Ok(found.map(|_| VolumeIndex(position))),
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                Err(error)
            }
        }
    }

    fn reader(&self, volume: VolumeIndex) -> Reader {
        let reached = self
            .reached
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(volume)
            .or_default()
            .clone();
        Reader {
            volumes: Arc::clone(&self.volumes),
            failed: Arc::clone(&self.failed),
            volume,
            position: 0,
            reached,
        }
    }

    // The furthest byte of the volume any read of the run reached.
    fn reached(&self, volume: VolumeIndex) -> u64 {
        self.reached
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&volume)
            .map_or(0, |reached| reached.load(Ordering::Relaxed))
    }
}

impl VolumeProvider for Input {
    fn get_volume(&self, index: usize) -> Result<Box<dyn ReadSeek>, VolumeProviderError> {
        let Ok(Some(volume)) = self.index(index) else {
            return Err(VolumeProviderError::Unavailable {
                volume: index,
                reason: "the set has no such volume".into(),
            });
        };
        Ok(Box::new(self.reader(volume)))
    }
}

struct Reader {
    volumes: Arc<dyn Volumes>,
    failed: Arc<AtomicBool>,
    volume: VolumeIndex,
    position: u64,
    reached: Arc<AtomicU64>,
}

impl Read for Reader {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        if into.is_empty() {
            return Ok(0);
        }
        match self.volumes.read_at(self.volume, self.position, into) {
            Ok(read) => {
                self.position += read as u64;
                self.reached.fetch_max(self.position, Ordering::Relaxed);
                Ok(read)
            }
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                Err(error)
            }
        }
    }
}

impl Seek for Reader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::Current(by) => self.position.checked_add_signed(by),
            // A growing volume has no end yet; unrar-rs reads a prefix
            // without one.
            SeekFrom::End(by) => self
                .volumes
                .len(self.volume)
                .ok_or_else(|| io::Error::from(io::ErrorKind::Unsupported))?
                .checked_add_signed(by),
        };
        self.position = position.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        Ok(self.position)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("the owner stopped the run")]
struct Stopped;

// The decoder's output, handed to the sink as it is written.
struct Out<'s, 'a> {
    sink: &'s mut Sink<'a>,
    member: MemberId,
    offset: u64,
    stopped: bool,
}

impl Write for Out<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let heard = (self.sink)(Emit::Bytes {
            member: self.member,
            offset: self.offset,
            bytes: Bytes::copy_from_slice(bytes),
            provenance: Provenance::Fresh,
        });
        if !heard {
            self.stopped = true;
            return Err(io::Error::other(Stopped));
        }
        self.offset += bytes.len() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
