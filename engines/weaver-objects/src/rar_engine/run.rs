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
    ArchiveFormat, CompressionMethod, RarArchive, RarError, ReadSeek, VolumeProvider,
    VolumeProviderError,
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

pub(crate) struct Run {
    password: Option<String>,
    // The store proved `password` before the run began: a failure under it
    // is the bytes', whatever the member's header checks.
    proven: bool,
    scope: Scope,
}

// What a run decodes, and which members it makes.
pub(crate) enum Scope {
    // Every member: each file body and directory is announced, numbered in
    // archive order.
    Whole,
    // The members the store's rule gives the run, as the run reads their
    // headers (`rar_store::made::by_run`), each made as the store's member
    // at the same archive index; every other member is passed over unread
    // and is no member of the run's. The names are those of the members the
    // store had laid when the run started, as the store's headers spell
    // them: the member the run reads at each of those indexes must carry
    // it. Nothing is announced: the namespace is the store's, and the run
    // only fills it.
    Members(BTreeMap<usize, String>),
}

impl Scope {
    // Whether a run of this scope makes member `at` of `archive`.
    fn makes(&self, archive: &RarArchive, at: usize) -> bool {
        match self {
            Scope::Whole => true,
            Scope::Members(_) => archive.member_info(at).is_some_and(|info| {
                let link = link(archive, at, &info);
                crate::rar_store::made::by_run(
                    archive.is_solid(),
                    crate::rar_store::made::Traits::of_info(&info, link),
                )
            }),
        }
    }
}

impl Run {
    pub(super) fn new(password: Option<String>) -> Self {
        Self::of(password, false, Scope::Whole)
    }

    pub(crate) fn of(password: Option<String>, proven: bool, scope: Scope) -> Self {
        Self {
            password,
            proven,
            scope,
        }
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
            password: self.password.as_deref(),
            proven: self.proven,
            sink,
            budget,
            next: 0,
            open: None,
            reading: 0,
            scope: self.scope,
        };
        let Err(Stop::Say(last)) = walk.archive() else {
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
    // Where an encrypted RAR5 member's header can be read again, to learn
    // whether it checks the password: RAR4 states no check.
    keyed: Option<Keyed>,
    // The volumes the member's data spans: from the one its header is in to
    // the furthest its decode opened, which is as far as the decode found
    // it, whatever headers the walk had read when it began.
    first: usize,
    last: usize,
    // Where a member a scoped run makes has its packed data, as far as the
    // archive has read its headers: all a failure of its decode can say is
    // damaged.
    data: Vec<(VolumeIndex, ByteRange)>,
}

// An encrypted RAR5 member's header: in volume `volume`, before its data
// at `data`, named `name`.
struct Keyed {
    volume: usize,
    data: u64,
    name: String,
}

struct Walk<'s, 'a> {
    input: Input,
    password: Option<&'s str>,
    proven: bool,
    sink: &'s mut Sink<'a>,
    budget: Budget,
    // The id the next member announced gets: members are numbered in
    // archive order, so every run of the same set numbers them alike.
    next: u32,
    open: Option<Open>,
    // The volume whose headers are being read.
    reading: usize,
    scope: Scope,
}

impl Walk<'_, '_> {
    fn archive(&mut self) -> Result<(), Stop> {
        let password = self.password;
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
                    self.side_by_side(&archive, index..index + batch)?;
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

    // Whether the run makes member `at`: every member of a whole run, and
    // of a scoped one those its scope names.
    fn makes(&self, archive: &RarArchive, at: usize) -> bool {
        self.scope.makes(archive, at)
    }

    // How many members from `index` on may decode side by side.
    fn batch(&self, archive: &RarArchive, index: usize, has_password: bool) -> usize {
        let mut held = 0;
        (index..archive.len())
            .map_while(|at| {
                // A member the run does not make is passed over unread.
                if self.makes(archive, at) {
                    held = may_start(archive, &self.input, &self.budget, at, held, has_password)?;
                }
                Some(())
            })
            .count()
    }

    // Decode the members of `batch` the run makes on as many of the lent
    // pool's threads as the budget grants, each member over an archive of
    // its own holding that member alone, and say them in archive order once
    // each is whole.
    fn side_by_side(
        &mut self,
        archive: &RarArchive,
        batch: std::ops::Range<usize>,
    ) -> Result<(), Stop> {
        let batch: Vec<usize> = batch.filter(|at| self.makes(archive, *at)).collect();
        if batch.is_empty() {
            return Ok(());
        }
        let slots: Vec<Mutex<Option<Decoded>>> = batch.iter().map(|_| Mutex::new(None)).collect();
        let workers = self.budget.threads().min(slots.len());
        let decode_limits = limits(&self.budget);
        // Each member's own archive, made on the walk's thread.
        let owns: Vec<Mutex<Option<RarArchive>>> = batch
            .iter()
            .map(|at| Mutex::new(Some(copy(archive, *at, &decode_limits))))
            .collect();
        let taken = AtomicUsize::new(0);
        let halted = AtomicBool::new(false);
        let volumes = &self.input.volumes;
        let work = || {
            let input = Input::beside(volumes);
            // Members are taken in archive order, so every one not taken
            // follows one that failed.
            while !halted.load(Ordering::Acquire) {
                let at = taken.fetch_add(1, Ordering::AcqRel);
                let (Some(slot), Some(own)) = (slots.get(at), owns.get(at)) else {
                    break;
                };
                let Some(mut own) = own.lock().unwrap_or_else(PoisonError::into_inner).take()
                else {
                    unreachable!("each member's archive is taken once");
                };
                let decoded = decode(&mut own, &input);
                if decoded.ended.is_some() {
                    halted.store(true, Ordering::Release);
                }
                *slot.lock().unwrap_or_else(PoisonError::into_inner) = Some(decoded);
            }
        };
        self.budget.pool().spread(workers, &work);
        for (at, slot) in batch.into_iter().zip(slots) {
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
        let member = self.announce(archive, at, &info)?;
        self.deliver(member, decoded.bytes)?;
        if let Some((error, read_failed)) = decoded.ended {
            self.data_of(archive, at);
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
    // A scoped run announces nothing: the member at archive index `at` is
    // the store's, which must name the member the run reads there.
    fn announce(
        &mut self,
        archive: &RarArchive,
        at: usize,
        info: &unrar_rs::MemberInfo,
    ) -> Result<MemberId, Stop> {
        let member = match &self.scope {
            Scope::Whole => {
                let member = MemberId(self.next);
                self.next += 1;
                self.say(Emit::Member {
                    member,
                    name: unrar_rs::sanitize_path(&info.raw_name),
                    length: info.unpacked_size,
                })?;
                member
            }
            Scope::Members(named) => match named.get(&at) {
                // The store's headers and the run's disagree on what the
                // member is: nothing the run would make there is the
                // store's member.
                Some(name) if *name != info.raw_name => {
                    return Err(Stop::Say(vec![Emit::Refuse(Why::Unsupported)]));
                }
                _ => MemberId(at as u32),
            },
        };
        let keyed = (info.is_encrypted && archive.format() == ArchiveFormat::Rar5)
            .then(|| archive.member_segments(at)?.first())
            .flatten()
            .map(|segment| Keyed {
                volume: segment.volume_index,
                data: segment.data_offset,
                name: info.raw_name.clone(),
            });
        self.open = Some(Open {
            member,
            encrypted: info.is_encrypted,
            keyed,
            first: info.volumes.first_volume,
            last: info.volumes.first_volume,
            data: Vec::new(),
        });
        self.data_of(archive, at);
        Ok(member)
    }

    // Where the open member's packed data lies, as far as `archive` read
    // its headers, for a member a scoped run makes.
    fn data_of(&mut self, archive: &RarArchive, at: usize) {
        if matches!(self.scope, Scope::Whole) {
            return;
        }
        let (Some(open), Some(segments)) = (&mut self.open, archive.member_segments(at)) else {
            return;
        };
        open.data = segments
            .iter()
            .filter_map(|segment| {
                Some((
                    VolumeIndex(u32::try_from(segment.volume_index).ok()?),
                    ByteRange::at(segment.data_offset, segment.data_size),
                ))
            })
            .collect();
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
        if link(archive, index, &info) {
            return Ok(1);
        }
        // A member the run does not make is passed over unread.
        if !self.makes(archive, index) {
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
        let member = self.announce(archive, index, &info)?;
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
            Err(error) => {
                self.data_of(archive, index);
                Err(self.failed_through(last, error))
            }
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
        let member = self.announce(archive, index, info)?;
        let decode_limits = limits(&self.budget);
        let mut own = copy(archive, index, &decode_limits);
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
                scope,
                ..
            } = &mut *self;
            let (input, budget) = (&*input, &*budget);
            let lend = budget.threads() - 1;
            let work = || {
                beside.work(&input.volumes);
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
                        // A member the run does not make is passed over
                        // unread; it is said by no one here.
                        if next < archive.len() && !scope.makes(archive, next) {
                            next += 1;
                            continue;
                        }
                        let Some(total) =
                            may_start(archive, input, budget, next, held, password.is_some())
                        else {
                            break;
                        };
                        beside.hand_over(next, copy(archive, next, &decode_limits));
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
                    .by_index_via(0, &reach)
                    .and_then(|entry| entry.copy_to(&mut out));
                last = reach.last();
                let decoded = out.flush().map_err(RarError::from).and(decoded);
                stopped = out.stopped;
                match decoded.is_ok() {
                    // Members handed over and not yet taken, on a pool that
                    // lends no thread beside its caller, decode here.
                    true => while beside.work(&input.volumes) {},
                    false => beside.halted.store(true, Ordering::Release),
                }
                inline = Some(decoded);
            });
        }
        match inline {
            Some(Ok(_)) => {}
            Some(Err(_)) if stopped => return Err(Stop::Silent),
            Some(Err(error)) => {
                self.data_of(&own, 0);
                return Err(self.failed_through(last, error));
            }
            None => unreachable!("a pool runs its caller"),
        }
        self.open = None;
        self.checksum_of(&own, 0, member)?;
        self.say(Emit::MemberClosed(member))?;
        let mut decoded = beside
            .decoded
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        for at in index + 1..next {
            if !self.makes(archive, at) {
                continue;
            }
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
        // A failure on an encrypted member asks for another password only
        // while the password is unproven.
        let unproven = self
            .open
            .as_ref()
            .is_some_and(|open| open.encrypted && !self.proven(open));
        let (first, last) = self
            .open
            .as_ref()
            .map_or((self.reading, self.reading), |open| (open.first, open.last));
        // A member a scoped run makes is damaged in its own packed data,
        // not in the volumes around it, which hold the store's members.
        let data = match (&self.scope, &self.open) {
            (Scope::Members(_), Some(open)) => Some(open.data.as_slice()),
            _ => None,
        };
        let mut said = vec![Emit::Failed(crate::verdict::failed(&said_of(&error)))];
        said.extend(verdict(&error, unproven, first..=last, data, &self.input));
        Stop::Say(said)
    }

    // Whether the run's password is the one the open member was enciphered
    // under: the store proved it before the run began, or the member's RAR5
    // header carries a password check value and the password reproduces it.
    // A failure after that is the bytes', not the password's. Otherwise
    // nothing proved it: a RAR4 header, and a RAR5 header with no check
    // value, prove nothing of their own, as the store keys such a file on
    // trial. The header is read again from the bytes before the member's
    // data, which the run has read already, so nothing waits on bytes still
    // to come.
    fn proven(&self, open: &Open) -> bool {
        if self.proven {
            return true;
        }
        let (Some(keyed), Some(password)) = (&open.keyed, self.password) else {
            return false;
        };
        let Ok(Some(volume)) = self.input.index(keyed.volume) else {
            return false;
        };
        let headers = Headers {
            reader: self.input.reader(volume),
            end: keyed.data,
        };
        let Ok(walk) = RarArchive::parse_volume_facts_walk(headers, Some(password)) else {
            return false;
        };
        let Some(encryption) = walk
            .facts
            .members
            .iter()
            .rev()
            .find(|member| !member.split_before && member.name == keyed.name)
            .and_then(|member| member.encryption.as_ref())
        else {
            return false;
        };
        unrar_rs::check_member_password(
            &unrar_rs::KdfCache::new(),
            password,
            &encryption.salt,
            encryption.kdf_count_lg2,
            encryption.psw_check.as_ref(),
        ) == unrar_rs::PasswordCheck::Verified
    }
}

// The error as the job's log may show it. unrar-rs names members, offsets
// and sizes, never a password; an I/O error is the owner's read of a volume
// or the allocator's, and only its kind is told, or the RAR error it
// carries.
fn said_of(error: &RarError) -> String {
    let carried = |error: &io::Error| {
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<RarError>())
            .map_or_else(|| error.kind().to_string(), said_of)
    };
    match error {
        RarError::Io(error) => format!("I/O error: {}", carried(error)),
        RarError::WindowAllocatorFailed { bytes, error } => {
            format!(
                "window allocator could not supply {bytes} bytes: {}",
                carried(error)
            )
        }
        error => error.to_string(),
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
    let plain =
        !(link(archive, at, &info) || info.is_directory || (info.is_encrypted && !has_password));
    let whole = after.volumes.first_volume >= *span.end();
    let settled = span.clone().all(|volume| input.settled(volume));
    let length = info.unpacked_size.filter(|_| plain && whole && settled)?;
    let total = held.checked_add(length)?.checked_add(window(&info))?;
    budget
        .memory_bytes
        .is_none_or(|memory| total <= memory)
        .then_some(total)
}

// What member `at` of `archive`, over `volumes`, holds once it decodes beside
// nothing, if it may.
#[cfg(test)]
pub(super) fn may_start_alone(
    archive: &RarArchive,
    volumes: &Arc<dyn Volumes>,
    budget: &Budget,
    at: usize,
) -> Option<u64> {
    may_start(archive, &Input::beside(volumes), budget, at, 0, false)
}

// The member count of each copy the run's thread built, in the order built.
#[cfg(test)]
pub(super) mod copied {
    use std::cell::RefCell;

    thread_local! {
        static COPIES: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
    }

    pub(in super::super) fn add(members: usize) {
        COPIES.with(|copies| copies.borrow_mut().push(members));
    }

    // The counts since the last take, which starts the next.
    pub(in super::super) fn take() -> Vec<usize> {
        COPIES.with(|copies| std::mem::take(&mut *copies.borrow_mut()))
    }
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

// An archive of its own holding member `at` of the walk's alone, for that
// member decoded apart from the walk's archive.
fn copy(archive: &RarArchive, at: usize, decode_limits: &unrar_rs::Limits) -> RarArchive {
    let Some(mut own) = archive.member_archive(at) else {
        unreachable!("a member the walk said is in its archive");
    };
    #[cfg(test)]
    copied::add(own.len());
    own.set_limits(decode_limits.clone());
    own.set_decode_mode(unrar_rs::DecodeMode::Adaptive);
    own.set_verify(true);
    own
}

// The member `own` holds decoded whole through `input`.
fn decode(own: &mut RarArchive, input: &Input) -> Decoded {
    let length = own
        .member_info(0)
        .and_then(|info| info.unpacked_size)
        .unwrap_or(0);
    let first = own
        .member_info(0)
        .map_or(0, |info| info.volumes.first_volume);
    let mut bytes = Vec::with_capacity(usize::try_from(length).unwrap_or_default());
    let reach = Reach::new(input, first);
    let ended = own
        .by_index_via(0, &reach)
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
// over with an archive of its own, and each start of the work takes one, in
// archive order, so every member a halt drops follows one that failed.
#[derive(Default)]
struct Beside {
    handed: Mutex<VecDeque<(usize, RarArchive)>>,
    decoded: Mutex<BTreeMap<usize, Decoded>>,
    // Members handed over and not yet decoded.
    running: AtomicUsize,
    halted: AtomicBool,
}

impl Beside {
    fn hand_over(&self, at: usize, own: RarArchive) {
        self.running.fetch_add(1, Ordering::AcqRel);
        self.handed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back((at, own));
    }

    // Decode the next member handed over, through a failure view of its
    // own. Says whether there was one.
    fn work(&self, volumes: &Arc<dyn Volumes>) -> bool {
        let taken = self
            .handed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front();
        let Some((at, mut own)) = taken else {
            return false;
        };
        if !self.halted.load(Ordering::Acquire) {
            let decoded = decode(&mut own, &Input::beside(volumes));
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
// as they were posted: on an encrypted member whose password is unproven, a
// wrong key decrypts to bytes the decoder or the checksum rejects, and the
// failure asks for another password before it is called damage. A member
// whose header checks the password and passed it fails on its bytes.
fn verdict(
    error: &RarError,
    unproven: bool,
    spans: std::ops::RangeInclusive<usize>,
    data: Option<&[(VolumeIndex, ByteRange)]>,
    input: &Input,
) -> Vec<Emit> {
    match rar_error_class(error) {
        Class::PasswordRequired => vec![Emit::NeedsPassword],
        Class::Recoverable if unproven => vec![Emit::NeedsPassword],
        Class::Recoverable => {
            // A closed volume is damaged whole; a growing one as far as the
            // run read it, for that is all the run can say of it. A volume
            // the run never read says nothing. A member a scoped run makes
            // is damaged only in its own packed data, as far as that.
            spans
                .filter_map(|position| {
                    let volume = input.index(position).ok()??;
                    let known_end = input
                        .volumes
                        .len(volume)
                        .unwrap_or_else(|| input.reached(volume));
                    (known_end > 0).then_some((volume, known_end))
                })
                .flat_map(|(volume, known_end)| {
                    let whole = [ByteRange::new(0, known_end)];
                    let ranges: Vec<ByteRange> = match data {
                        None => whole.to_vec(),
                        Some(data) => data
                            .iter()
                            .filter(|(at, _)| *at == volume)
                            .map(|(_, range)| ByteRange::new(range.start, range.end.min(known_end)))
                            .filter(|range| !range.is_empty())
                            .collect(),
                    };
                    ranges
                        .into_iter()
                        .map(move |range| Emit::Damaged { volume, range })
                })
                .collect()
        }
        Class::Terminal => vec![terminal(error, unproven)],
    }
}

// A member that redirects elsewhere holds no file body. A redirection of a
// type unrar does not know carries none of the three flags, and is one too.
fn link(archive: &RarArchive, index: usize, info: &unrar_rs::MemberInfo) -> bool {
    info.is_symlink
        || info.is_hardlink
        || info.is_file_copy
        || matches!(archive.member_redirection(index), Ok(Some(_)))
}

fn terminal(error: &RarError, unproven: bool) -> Emit {
    match error {
        // A wrong key's bytes, as the decoder rejects them.
        RarError::Blake2Mismatch { .. }
        | RarError::CorruptArchive { .. }
        | RarError::InvalidHuffmanTable
            if unproven =>
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
            // A member that runs on past the set's last volume is short of
            // one: the owner knows the set ended, and rules on the open
            // member once the job is quiet, so the run ends silently. A set
            // whose volumes sit apart gives them up there to be lifted.
            self.failed.store(true, Ordering::Release);
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

// A volume as far as a member's data: the headers before it. The walk
// over it seeks past the data of the members before, so it reads headers
// only, which are all in once the member's own header was read.
struct Headers {
    reader: Reader,
    end: u64,
}

impl Read for Headers {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        let left = self.end.saturating_sub(self.reader.position);
        let take = into.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        self.reader.read(&mut into[..take])
    }
}

impl Seek for Headers {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let from = match from {
            SeekFrom::End(by) => SeekFrom::Start(
                self.end
                    .checked_add_signed(by)
                    .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?,
            ),
            from => from,
        };
        self.reader.seek(from)
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
