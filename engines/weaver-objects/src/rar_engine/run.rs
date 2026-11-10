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
// While a member decodes on the run's thread, waiting on bytes still to
// come, the walk reads on the headers of the volumes already in and the
// files there decode beside it, said once it closes.

use std::collections::{BTreeMap, VecDeque};
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

// A member decoded whole beside others: the bytes it came to, the furthest
// volume its decode opened, and, if it failed, why and whether a read of its
// volumes failed.
struct Decoded {
    bytes: Vec<u8>,
    last: usize,
    ended: Option<(RarError, bool)>,
}

// The smallest piece of a member the sink hears, its last aside, and the
// largest of a member decoded whole. A streamed decoder writes a few hundred
// kilobytes at a time, with a few hundred bytes beside each window it
// flushes; each piece the sink hears costs a message, a write and the
// bookkeeping behind both, so smaller writes gather into one first.
pub(super) const EMIT_BYTES: usize = 1 << 20;

pub(super) struct Run {
    password: Option<String>,
}

impl Run {
    pub(super) fn new(password: Option<String>) -> Self {
        Self { password }
    }
}

// A member's dictionary is held to the window the run was granted, or the
// decoder's own limit when none was. A RAR3 PPMd arena keeps the format's
// own ceiling, narrowed by the window too. A run handed a lender borrows a
// dictionary or an arena over that window: `crate::lent::rar`.
pub(super) fn limits(budget: &Budget) -> unrar_rs::Limits {
    crate::lent::rar(budget)
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
    // The volumes the member's data spans: from the one its header is in to
    // the furthest its decode opened, which is as far as the decode found
    // it, whatever headers the walk had read when it began.
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
        archive.set_limits(limits(&self.budget));
        // Bytes may catch up with a large member as it decodes; adaptive
        // decoding picks inline or batched work from what it measures.
        archive.set_decode_mode(unrar_rs::DecodeMode::Adaptive);
        archive.set_verify(true);
        let spreads = self.spreads(&archive);
        let mut cursor = Cursor::new(0, first, &self.input, &self.budget, spreads);
        let mut index = 0;
        loop {
            let wanted = cursor
                .headers
                .checked_add(cursor.step.get())
                .and_then(NonZeroUsize::new)
                .ok_or(Stop::Say(vec![Emit::Refuse(Why::Unsupported)]))?;
            self.reading = cursor.volume;
            cursor
                .read(&mut archive, &self.input, wanted)
                .map_err(|error| self.fail(error))?;
            while index < archive.len() {
                let batch = match spreads {
                    true => self.batch(&archive, index, password.is_some()),
                    false => 0,
                };
                // Members that reach the last header read, whose end no
                // later header shows yet, wait for the next ones while
                // fewer would decode beside each other than the threads
                // allow and reading on waits for nothing: the rest of a
                // volume whose bytes are all in, or the next volume once
                // its bytes are. A member that runs on into the next
                // volume then decodes beside those that follow.
                let reads_on = match cursor.finished {
                    false => self.input.settled(cursor.volume),
                    true => self.input.settled(cursor.volume + 1),
                };
                if cursor.step.get() > 1
                    && batch < cursor.step.get()
                    && index + batch + 1 == archive.len()
                    && reads_on
                {
                    break;
                }
                if batch > 1 {
                    self.side_by_side(&archive, index..index + batch, password)?;
                    index += batch;
                    continue;
                }
                index += self.member(&mut archive, index, &mut cursor, password)?;
            }
            if !cursor.finished {
                continue;
            }
            let volume = cursor.volume + 1;
            let Some(at) = self.input.index(volume).map_err(|_| Stop::Silent)? else {
                break;
            };
            cursor = Cursor::new(volume, at, &self.input, &self.budget, spreads);
        }
        // A last header left waiting for a volume the set turned out not to
        // have decodes alone.
        while index < archive.len() {
            index += self.member(&mut archive, index, &mut cursor, password)?;
        }
        Ok(())
    }

    // Whether members may decode beside each other: those of a set that is
    // not solid, on more than the run's own thread.
    fn spreads(&self, archive: &RarArchive) -> bool {
        self.budget.threads() > 1 && !archive.is_solid()
    }

    // How many members from `index` on may decode side by side.
    fn batch(&self, archive: &RarArchive, index: usize, has_password: bool) -> usize {
        let mut held = 0;
        (index..archive.len())
            .map_while(|at| {
                held = may_start(archive, &self.input, &self.budget, at, held, has_password)?;
                Some(())
            })
            .count()
    }

    // Decode the members of `batch` on as many of the lent pool's threads
    // as the budget grants, each worker over an archive of its own made from
    // the headers the walk parsed, and say them in archive order once each
    // is whole.
    fn side_by_side(
        &mut self,
        archive: &RarArchive,
        batch: std::ops::Range<usize>,
        password: Option<&str>,
    ) -> Result<(), Stop> {
        let slots: Vec<Mutex<Option<Decoded>>> = batch.clone().map(|_| Mutex::new(None)).collect();
        let workers = self.budget.threads().min(slots.len());
        // The walk's headers, read once, one copy for each worker.
        let headers: Mutex<Vec<CachedArchiveHeaders>> =
            Mutex::new((0..workers).map(|_| archive.export_headers()).collect());
        let taken = AtomicUsize::new(0);
        let halted = AtomicBool::new(false);
        let (volumes, kdf, start) = (&self.input.volumes, &self.kdf, batch.start);
        let decode_limits = limits(&self.budget);
        let work = || {
            let Some(headers) = headers.lock().unwrap_or_else(PoisonError::into_inner).pop() else {
                return;
            };
            let mut own = copy(headers, password, kdf, &decode_limits);
            let input = Input::beside(volumes);
            // Members are taken in archive order, so every one not taken
            // follows one that failed.
            while !halted.load(Ordering::Acquire) {
                let at = taken.fetch_add(1, Ordering::AcqRel);
                let Some(slot) = slots.get(at) else {
                    break;
                };
                let decoded = decode(&mut own, start + at, &input);
                if decoded.ended.is_some() {
                    halted.store(true, Ordering::Release);
                }
                *slot.lock().unwrap_or_else(PoisonError::into_inner) = Some(decoded);
            }
        };
        self.budget.pool().spread(workers, &work);
        for (at, slot) in batch.zip(slots) {
            match slot.into_inner().unwrap_or_else(PoisonError::into_inner) {
                Some(decoded) => self.say_decoded(archive, at, decoded)?,
                None => unreachable!("a member not taken follows one that failed"),
            }
        }
        Ok(())
    }

    // A member decoded beside others, said as it would have been decoding
    // alone: its bytes as far as its decode came, then its close, or the
    // failure that ended it.
    fn say_decoded(
        &mut self,
        archive: &RarArchive,
        at: usize,
        decoded: Decoded,
    ) -> Result<(), Stop> {
        let Some(info) = archive.member_info(at) else {
            return Err(self.fail(RarError::MemberIndexOutOfRange {
                index: at,
                len: archive.len(),
            }));
        };
        let member = self.announce(&info)?;
        self.deliver(member, decoded.bytes)?;
        if let Some((error, read_failed)) = decoded.ended {
            // Decoding alone, the walk would have read what failed itself.
            if read_failed {
                self.input.failed.store(true, Ordering::Release);
            }
            return Err(self.failed_through(decoded.last, error));
        }
        self.open = None;
        self.checksum_of(archive, at, member)?;
        self.say(Emit::MemberClosed(member))
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
            last: info.volumes.first_volume,
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
        let bytes = Bytes::from(bytes);
        let mut offset = 0;
        while offset < bytes.len() {
            let end = bytes.len().min(offset + EMIT_BYTES);
            self.say(Emit::Bytes {
                member,
                offset: offset as u64,
                bytes: bytes.slice(offset..end).into(),
                provenance: Provenance::Fresh,
            })?;
            offset = end;
        }
        Ok(())
    }

    // Decode member `index` on the run's thread, straight into the sink, and
    // say how many members were said: with threads to spare, members after
    // it decode beside it and are said after it.
    fn member(
        &mut self,
        archive: &mut RarArchive,
        index: usize,
        cursor: &mut Cursor,
        password: Option<&str>,
    ) -> Result<usize, Stop> {
        let Some(info) = archive.member_info(index) else {
            return Err(self.fail(RarError::MemberIndexOutOfRange {
                index,
                len: archive.len(),
            }));
        };
        // A link is not a file body, so it is no member and the files around
        // it stand.
        if info.is_symlink || info.is_hardlink || info.is_file_copy {
            return Ok(1);
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
            return self.say(Emit::MemberClosed(member)).map(|()| 1);
        }
        if info.is_encrypted && password.is_none() {
            return Err(Stop::Say(vec![Emit::NeedsPassword]));
        }
        if self.spreads(archive) {
            return self.alongside(archive, index, &info, cursor, password);
        }
        let member = self.announce(&info)?;
        let mut out = Out {
            sink: &mut *self.sink,
            member,
            offset: 0,
            gathered: Vec::new(),
            stopped: false,
            between: &mut || {},
        };
        let reach = Reach::new(&self.input, info.volumes.first_volume);
        let decoded = archive
            .by_index_via(index, &reach)
            .and_then(|entry| entry.copy_to(&mut out));
        let last = reach.last();
        // What is gathered is said whether or not the decode went on to
        // fail, as each write was before it was gathered.
        let decoded = out.flush().map_err(RarError::from).and(decoded);
        match decoded {
            Ok(_) => {
                self.open = None;
                self.checksum_of(archive, index, member)?;
                self.say(Emit::MemberClosed(member)).map(|()| 1)
            }
            Err(_) if out.stopped => Err(Stop::Silent),
            Err(error) => Err(self.failed_through(last, error)),
        }
    }

    // Decode member `index` on the run's thread over a copy of the walk's
    // headers, so the walk's own archive is free to read on. Between one
    // write and the next the walk reads the headers of the volumes whose
    // bytes are all in, and hands each member after it that may start to a
    // worker beside it, on as many threads as the budget grants beyond the
    // run's own. They are said once it closes, in archive order, as if
    // decoded one by one; if it fails, they are not said, as the walk would
    // not have reached them.
    fn alongside(
        &mut self,
        archive: &mut RarArchive,
        index: usize,
        info: &unrar_rs::MemberInfo,
        cursor: &mut Cursor,
        password: Option<&str>,
    ) -> Result<usize, Stop> {
        let member = self.announce(info)?;
        let decode_limits = limits(&self.budget);
        let mut own = copy(
            archive.export_headers(),
            password,
            &self.kdf,
            &decode_limits,
        );
        let beside = Beside::default();
        let mut next = index + 1;
        let mut inline = None;
        let mut stopped = false;
        let mut last = info.volumes.first_volume;
        {
            let Walk {
                input,
                sink,
                budget,
                kdf,
                ..
            } = &mut *self;
            let (input, budget) = (&*input, &*budget);
            let lend = budget.threads() - 1;
            let work = || {
                beside.work(&input.volumes, kdf, password, &decode_limits);
            };
            // The decoder's window is held while the member decodes; its
            // bytes are said as they come.
            let mut held = window(info);
            let mut ahead = Some(Input::beside(&input.volumes));
            budget.pool().spread_beside(&work, &mut |more| {
                let mut between = || {
                    if let Some(view) = &ahead
                        && !look_ahead(archive, view, budget, cursor)
                    {
                        ahead = None;
                    }
                    while beside.running.load(Ordering::Acquire) < lend {
                        let Some(total) =
                            may_start(archive, input, budget, next, held, password.is_some())
                        else {
                            break;
                        };
                        beside.hand_over(next, archive.export_headers());
                        held = total;
                        next += 1;
                        more();
                    }
                };
                between();
                let mut out = Out {
                    sink: &mut **sink,
                    member,
                    offset: 0,
                    gathered: Vec::new(),
                    stopped: false,
                    between: &mut between,
                };
                let reach = Reach::new(input, last);
                let decoded = own
                    .by_index_via(index, &reach)
                    .and_then(|entry| entry.copy_to(&mut out));
                last = reach.last();
                let decoded = out.flush().map_err(RarError::from).and(decoded);
                stopped = out.stopped;
                match decoded.is_ok() {
                    // Members handed over and not yet taken, on a pool that
                    // lends no thread beside its caller, decode here.
                    true => while beside.work(&input.volumes, kdf, password, &decode_limits) {},
                    false => beside.halted.store(true, Ordering::Release),
                }
                inline = Some(decoded);
            });
        }
        match inline {
            Some(Ok(_)) => {}
            Some(Err(_)) if stopped => return Err(Stop::Silent),
            Some(Err(error)) => return Err(self.failed_through(last, error)),
            None => unreachable!("a pool runs its caller"),
        }
        self.open = None;
        self.checksum_of(&own, index, member)?;
        self.say(Emit::MemberClosed(member))?;
        let mut decoded = beside
            .decoded
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        for at in index + 1..next {
            match decoded.remove(&at) {
                Some(decoded) => self.say_decoded(archive, at, decoded)?,
                None => unreachable!("a member a halt dropped follows one that failed"),
            }
        }
        Ok(next - index)
    }

    fn say(&mut self, emit: Emit) -> Result<(), Stop> {
        if (self.sink)(emit) {
            Ok(())
        } else {
            Err(Stop::Silent)
        }
    }

    // The open member failed, its decode having opened volumes as far as
    // `last`.
    fn failed_through(&mut self, last: usize, error: RarError) -> Stop {
        if let Some(open) = &mut self.open {
            open.last = last;
        }
        self.fail(error)
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

// Where the walk's reading of headers stands: the volume read, how many
// file headers each read of it asks for, how many it has read, and whether
// those are all.
struct Cursor {
    volume: usize,
    at: VolumeIndex,
    // A closed volume's headers are read as many ahead as members can
    // decode at once; a growing one's one at a time, so the walk never
    // waits on a header for a member it could decode now.
    step: NonZeroUsize,
    headers: usize,
    finished: bool,
}

impl Cursor {
    fn new(volume: usize, at: VolumeIndex, input: &Input, budget: &Budget, spreads: bool) -> Self {
        let step = match spreads && input.volumes.len(at).is_some() {
            true => NonZeroUsize::new(budget.threads()).unwrap_or(NonZeroUsize::MIN),
            false => NonZeroUsize::MIN,
        };
        Self {
            volume,
            at,
            step,
            headers: 0,
            finished: false,
        }
    }

    // Read the volume's headers on, as far as `wanted` of them.
    fn read(
        &mut self,
        archive: &mut RarArchive,
        input: &Input,
        wanted: NonZeroUsize,
    ) -> Result<(), RarError> {
        let found =
            archive.extend_volume_prefix(self.volume, Box::new(input.reader(self.at)), wanted)?;
        self.headers = found;
        self.finished = found < wanted.get();
        Ok(())
    }
}

// Whether member `at` may decode beside others with `held` already held,
// and what is held once it does: a file, of plain bytes or with the
// password its bytes need, whose data lies in volumes whose bytes are all
// in, so a worker on the lent pool never waits on a download, which a later
// header shows runs on no further, whose length is known, and which fits in
// the memory the budget grants beside what is held. It holds its bytes,
// decoded whole before it is said, and its decoder's window.
fn may_start(
    archive: &RarArchive,
    input: &Input,
    budget: &Budget,
    at: usize,
    held: u64,
    has_password: bool,
) -> Option<u64> {
    let (info, after) = (archive.member_info(at)?, archive.member_info(at + 1)?);
    let span = info.volumes.first_volume..=info.volumes.last_volume;
    let plain = !(info.is_symlink
        || info.is_hardlink
        || info.is_file_copy
        || info.is_directory
        || (info.is_encrypted && !has_password));
    let whole = after.volumes.first_volume >= *span.end();
    let settled = span.clone().all(|volume| input.settled(volume));
    let length = info.unpacked_size.filter(|_| plain && whole && settled)?;
    let total = held.checked_add(length)?.checked_add(window(&info))?;
    budget
        .memory_bytes
        .is_none_or(|memory| total <= memory)
        .then_some(total)
}

// The window a member's decoder holds: as large as the dictionary its
// header names, none for stored bytes.
fn window(info: &unrar_rs::MemberInfo) -> u64 {
    match info.compression.method {
        CompressionMethod::Store => 0,
        _ => info.compression.dict_size,
    }
}

// Read on every header of the volumes from the cursor on whose bytes are
// all in, while a member decodes on the run's thread: such reads never
// wait. Says whether to go on: a read that fails ends it, and the walk
// meets that failure again when it reads there itself.
fn look_ahead(
    archive: &mut RarArchive,
    input: &Input,
    budget: &Budget,
    cursor: &mut Cursor,
) -> bool {
    loop {
        if !cursor.finished {
            if !input.settled(cursor.volume) {
                return true;
            }
            if cursor.read(archive, input, NonZeroUsize::MAX).is_err() {
                return false;
            }
            continue;
        }
        let volume = cursor.volume + 1;
        if !input.settled(volume) {
            return true;
        }
        let Ok(Some(at)) = input.index(volume) else {
            return false;
        };
        *cursor = Cursor::new(volume, at, input, budget, true);
    }
}

// An archive of its own over the walk's headers, for a member decoded
// apart from the walk's archive.
fn copy(
    headers: CachedArchiveHeaders,
    password: Option<&str>,
    kdf: &Arc<KdfCache>,
    decode_limits: &unrar_rs::Limits,
) -> RarArchive {
    let mut own = RarArchive::from_cached_headers_with_password_and_shared_kdf_cache(
        headers,
        password.map(str::to_owned),
        Arc::clone(kdf),
    );
    own.set_limits(decode_limits.clone());
    own.set_decode_mode(unrar_rs::DecodeMode::Adaptive);
    own.set_verify(true);
    own
}

// Member `at` of `own` decoded whole through `input`.
fn decode(own: &mut RarArchive, at: usize, input: &Input) -> Decoded {
    let length = own
        .member_info(at)
        .and_then(|info| info.unpacked_size)
        .unwrap_or(0);
    let first = own
        .member_info(at)
        .map_or(0, |info| info.volumes.first_volume);
    let mut bytes = Vec::with_capacity(usize::try_from(length).unwrap_or_default());
    let reach = Reach::new(input, first);
    let ended = own
        .by_index_via(at, &reach)
        .and_then(|entry| entry.copy_to(&mut bytes))
        .err()
        .map(|error| (error, input.failed.load(Ordering::Acquire)));
    Decoded {
        bytes,
        last: reach.last(),
        ended,
    }
}

// One member's decode through the run's input, keeping the furthest volume
// it opened.
struct Reach<'i> {
    input: &'i Input,
    last: AtomicUsize,
}

impl<'i> Reach<'i> {
    fn new(input: &'i Input, first: usize) -> Self {
        Self {
            input,
            last: AtomicUsize::new(first),
        }
    }

    fn last(&self) -> usize {
        self.last.load(Ordering::Relaxed)
    }
}

impl VolumeProvider for Reach<'_> {
    fn get_volume(&self, index: usize) -> Result<Box<dyn ReadSeek>, VolumeProviderError> {
        self.last.fetch_max(index, Ordering::Relaxed);
        self.input.get_volume(index)
    }
}

// The members decoding beside the one on the run's thread. Each is handed
// over with a copy of the walk's headers, and each start of the work takes
// one, in archive order, so every member a halt drops follows one that
// failed.
#[derive(Default)]
struct Beside {
    handed: Mutex<VecDeque<(usize, CachedArchiveHeaders)>>,
    decoded: Mutex<BTreeMap<usize, Decoded>>,
    // Members handed over and not yet decoded.
    running: AtomicUsize,
    halted: AtomicBool,
}

impl Beside {
    fn hand_over(&self, at: usize, headers: CachedArchiveHeaders) {
        self.running.fetch_add(1, Ordering::AcqRel);
        self.handed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back((at, headers));
    }

    // Decode the next member handed over, through a failure view of its
    // own. Says whether there was one.
    fn work(
        &self,
        volumes: &Arc<dyn Volumes>,
        kdf: &Arc<KdfCache>,
        password: Option<&str>,
        decode_limits: &unrar_rs::Limits,
    ) -> bool {
        let taken = self
            .handed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front();
        let Some((at, headers)) = taken else {
            return false;
        };
        if !self.halted.load(Ordering::Acquire) {
            let mut own = copy(headers, password, kdf, decode_limits);
            let decoded = decode(&mut own, at, &Input::beside(volumes));
            if decoded.ended.is_some() {
                self.halted.store(true, Ordering::Release);
            }
            self.decoded
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(at, decoded);
        }
        self.running.fetch_sub(1, Ordering::AcqRel);
        true
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
        | RarError::ResourceLimit { .. }
        | RarError::VolumeProviderRequired { .. }
        | RarError::SolidOrderViolation { .. } => {
            Emit::Refuse(too_large(error).unwrap_or(Why::Unsupported))
        }
        _ => Emit::Refuse(too_large(error).unwrap_or(Why::Unrepairable)),
    }
}

// A member whose decode needs more memory than the run may hold: a
// dictionary or a PPMd model over its limit, or memory the allocator or the
// run's lender would not give. A dictionary of no size names no limit either: the decoder
// refused a window it could not build.
fn too_large(error: &RarError) -> Option<Why> {
    let known = |size: u64| (size > 0).then_some(size);
    match error {
        RarError::DictionaryTooLarge { size, max } => Some(Why::TooLarge {
            needed: known(*size),
            allowed: known(*max),
        }),
        RarError::ResourceLimit {
            kind: unrar_rs::LimitKind::PpmdArena,
            requested,
            max,
            ..
        } => Some(Why::TooLarge {
            needed: Some(*requested),
            allowed: Some(*max),
        }),
        RarError::AllocationFailed { bytes } => Some(Why::TooLarge {
            needed: known(*bytes),
            allowed: None,
        }),
        // A window or arena the run's lender refused.
        RarError::WindowAllocatorFailed { bytes, error } => {
            Some(crate::lent::refused(*bytes, error.kind(), None))
        }
        RarError::Io(error) => too_large(error.get_ref()?.downcast_ref::<RarError>()?),
        _ => None,
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
    // The same volumes with a failure of their own to remember, for work
    // beside the walk: a read it fails tells the walk nothing until the walk
    // reaches the member, in archive order.
    fn beside(volumes: &Arc<dyn Volumes>) -> Self {
        Self {
            volumes: Arc::clone(volumes),
            failed: Arc::new(AtomicBool::new(false)),
            reached: Mutex::default(),
        }
    }

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

    // Whether the volume at `position` is in the set and no read of it waits.
    fn settled(&self, position: usize) -> bool {
        u32::try_from(position).is_ok_and(|position| self.volumes.settled(VolumeIndex(position)))
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

// The decoder's output, handed to the sink in pieces of at least
// `EMIT_BYTES`, the member's last aside. A write that size or larger with
// nothing gathered goes to the sink as it is; anything smaller gathers until
// the pieces reach it. Either way each byte is copied once.
struct Out<'s, 'a, 'b> {
    sink: &'s mut Sink<'a>,
    member: MemberId,
    offset: u64,
    gathered: Vec<u8>,
    stopped: bool,
    // What the walk does between one write and the next.
    between: &'b mut dyn FnMut(),
}

impl Out<'_, '_, '_> {
    fn say(&mut self, bytes: Bytes) -> io::Result<()> {
        let length = bytes.len() as u64;
        let heard = (self.sink)(Emit::Bytes {
            member: self.member,
            offset: self.offset,
            bytes: bytes.into(),
            provenance: Provenance::Fresh,
        });
        if !heard {
            self.stopped = true;
            return Err(io::Error::other(Stopped));
        }
        self.offset += length;
        Ok(())
    }
}

impl Write for Out<'_, '_, '_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.gathered.is_empty() && bytes.len() >= EMIT_BYTES {
            self.say(Bytes::copy_from_slice(bytes))?;
        } else {
            self.gathered.extend_from_slice(bytes);
            if self.gathered.len() >= EMIT_BYTES {
                self.flush()?;
            }
        }
        (self.between)();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.gathered.is_empty() || self.stopped {
            return Ok(());
        }
        let gathered = std::mem::take(&mut self.gathered);
        self.say(Bytes::from(gathered))
    }
}
