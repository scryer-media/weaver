// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A repair's read of the object's volumes, through par3-rs's own packet
// scanner. Each readable span of a volume is one par3-rs source, read
// through the namespace a bounded piece at a time. A metadata packet is
// kept parsed; a data or recovery packet is kept only as where it lies,
// and read back a piece at a time when the rebuild spends it. No volume,
// span or block of recovery is held whole, so a span of any length is
// read and used.
//
// The sets are resolved from the metadata packets as a scan of the whole
// bytes resolved them, and each set's data and recovery packets are kept
// as that resolution kept them: in the order they were found, the first
// copy of each, and a recovery block only under its own set's root, with
// every block that contradicts another for one row of one matrix left out.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use par3_rs::ingest::{IngestedPacket, PacketScanner, PayloadKind, PayloadRef, ScanEvent};
use par3_rs::runtime::{ExecutionOptions, MemoryBudget, ScanWorkBudget};
use par3_rs::source::{SourceAccess, SourceId, SourceSnapshot};
use par3_rs::{Fingerprint, InputSetId, Packet, Par3Set, ScanLimits};
use weaver_graph::recovery::MemberAccess;
use weaver_graph::{ByteRange, VolumeIndex};

// The least one read of a volume through the namespace takes while a scan
// reads it, so a span is read in pieces of this, not a header at a time.
pub(super) const PIECE: u64 = 1 << 20;

// One recovery block of a set: the row of the matrix it is, whether the
// set carries that matrix, and where its bytes lie.
pub(super) struct Row {
    pub(super) index: u64,
    pub(super) matrix: Fingerprint,
    pub(super) present: bool,
    pub(super) payload: PayloadRef,
}

// What a set carries beyond its description: its recovery blocks, by index
// then matrix, and its input blocks carried as data, in the order found.
#[derive(Default)]
pub(super) struct Payloads {
    pub(super) recovery: Vec<Row>,
    pub(super) data: Vec<(u64, PayloadRef)>,
}

// The sets the volumes describe, each with what it carries, and the reader
// those are read back through.
pub(super) struct Read {
    pub(super) sets: Vec<Par3Set>,
    pub(super) payloads: Vec<Payloads>,
    pub(super) volumes: Arc<Volumes>,
}

// The readable spans of the volumes as par3-rs sources, the span numbered
// `n` being `SourceId(n)`, and the one piece of them held at a time.
pub(super) struct Volumes {
    spans: Vec<(VolumeIndex, ByteRange)>,
    window: Mutex<Window>,
}

// The piece held, and the read the last call found it does not hold.
#[derive(Default)]
struct Window {
    source: Option<SourceId>,
    start: u64,
    bytes: Vec<u8>,
    missed: Option<(SourceId, u64, u64)>,
}

impl Volumes {
    fn new(spans: Vec<(VolumeIndex, ByteRange)>) -> Self {
        Self {
            spans,
            window: Mutex::new(Window::default()),
        }
    }

    fn span(&self, source: SourceId) -> Option<ByteRange> {
        let at = usize::try_from(source.0).ok()?;
        self.spans.get(at).map(|(_, span)| *span)
    }

    fn window(&self) -> std::sync::MutexGuard<'_, Window> {
        self.window.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Hold the piece the last read missed, read through the namespace at
    // least `least` wide; only that read's own range when the wider one
    // does not read. Which read it held, or `None` once the bytes do not
    // read back.
    fn fill(&self, least: u64, access: &mut dyn MemberAccess) -> Option<(SourceId, u64, u64)> {
        let mut window = self.window();
        let missed = window.missed.take()?;
        let (source, offset, want) = missed;
        let at = usize::try_from(source.0).ok()?;
        let &(volume, span) = self.spans.get(at)?;
        let wide = want.max(least).min(span.len().saturating_sub(offset));
        let mut bytes = std::mem::take(&mut window.bytes);
        window.source = None;
        for len in [wide, want] {
            bytes.clear();
            let range = ByteRange::at(span.start + offset, len);
            if access.read_volume(volume, range, &mut bytes) && bytes.len() as u64 == len {
                window.source = Some(source);
                window.start = offset;
                window.bytes = bytes;
                return Some(missed);
            }
            if len == want {
                break;
            }
        }
        window.bytes = bytes;
        None
    }

    // Read `out.len()` bytes of the payload from `offset` through the
    // namespace, no more than asked. Whether all of them read.
    pub(super) fn read(
        &self,
        payload: &PayloadRef,
        offset: u64,
        out: &mut [u8],
        access: &mut dyn MemberAccess,
    ) -> bool {
        let mut last = None;
        loop {
            self.window().missed = None;
            match payload.read_at(offset, out) {
                Ok(read) => return read == out.len(),
                Err(_) => {
                    let missed = self.window().missed;
                    if missed.is_none() || missed == last {
                        return false;
                    }
                    last = self.fill(0, access);
                    if last.is_none() {
                        return false;
                    }
                }
            }
        }
    }

    // The packets of one span in order, or `None` when the span does not
    // read back whole or the scan fails: such a span contributes nothing.
    // A packet that authenticates and does not parse is stepped past, as
    // the scanner does.
    fn scan(
        &self,
        source: &Arc<dyn SourceAccess>,
        id: SourceId,
        options: &ExecutionOptions,
        limits: ScanLimits,
        access: &mut dyn MemberAccess,
    ) -> Option<Vec<IngestedPacket>> {
        let mut scanner =
            PacketScanner::new(Arc::clone(source), id, options.clone(), limits).ok()?;
        let mut found = Vec::new();
        let mut last = None;
        loop {
            self.window().missed = None;
            let at = scanner.position();
            match scanner.poll() {
                Ok(ScanEvent::Packet(packet)) => found.push(packet),
                Ok(ScanEvent::End) => return Some(found),
                Ok(ScanEvent::NeedData { .. }) => {
                    let missed = self.window().missed;
                    if missed.is_none() || missed == last {
                        return None;
                    }
                    last = Some(self.fill(PIECE, access)?);
                }
                Err(_) if scanner.position() != at => {}
                Err(_) => return None,
            }
        }
    }
}

impl SourceAccess for Volumes {
    fn snapshot(&self, source: SourceId) -> io::Result<Option<SourceSnapshot>> {
        Ok(self.span(source).map(|span| SourceSnapshot {
            len: span.len(),
            generation: 0,
        }))
    }

    // A read is answered whole from the piece held, or not at all, and the
    // read is noted for `fill`.
    fn read_at(&self, source: SourceId, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        let Some(span) = self.span(source) else {
            return Ok(0);
        };
        let want = span.len().saturating_sub(offset).min(out.len() as u64);
        if want == 0 {
            return Ok(0);
        }
        let mut window = self.window();
        let held = window.source == Some(source)
            && window.start <= offset
            && offset + want <= window.start + window.bytes.len() as u64;
        if !held {
            window.missed = Some((source, offset, want));
            return Ok(0);
        }
        let from = (offset - window.start) as usize;
        let want = want as usize;
        out[..want].copy_from_slice(&window.bytes[from..from + want]);
        Ok(want)
    }

    // Every byte of a readable span arrived.
    fn next_available(&self, source: SourceId, offset: u64) -> io::Result<Option<Range<u64>>> {
        Ok(self
            .span(source)
            .map(|span| span.len())
            .filter(|len| offset < *len)
            .map(|len| offset..len))
    }
}

// The scanner's own bounds, at the most the scan of the whole bytes let
// one span keep, and no budget beside them: what a scan keeps is metadata,
// and the rest is read where it lies.
fn options() -> ExecutionOptions {
    let mut options = ExecutionOptions::default();
    options.memory = MemoryBudget::new(usize::MAX);
    options.scan_work = ScanWorkBudget::new(u64::MAX);
    options.retained_bytes =
        usize::try_from(ScanLimits::DEFAULT_MAX_RETAINED_BYTES).unwrap_or(usize::MAX);
    options
}

// Every set the readable spans of the volumes describe, read through the
// namespace, with what each carries. A job of more sets than `max_sets`
// has none, as one whose packets describe no set whole does.
pub(super) fn read(
    spans: Vec<(VolumeIndex, ByteRange)>,
    limits: ScanLimits,
    max_sets: usize,
    access: &mut dyn MemberAccess,
) -> Read {
    let volumes = Arc::new(Volumes::new(spans));
    let source: Arc<dyn SourceAccess> = volumes.clone();
    let options = options();
    let mut seen: HashSet<Fingerprint> = HashSet::new();
    let mut metadata: Vec<Packet> = Vec::new();
    let mut carried: BTreeMap<InputSetId, Vec<PayloadRef>> = BTreeMap::new();
    for at in 0..volumes.spans.len() {
        let id = SourceId(at as u64);
        let Some(found) = volumes.scan(&source, id, &options, limits, access) else {
            continue;
        };
        for packet in found {
            if !seen.insert(packet.hash()) {
                continue;
            }
            if let Some(packet) = packet.metadata() {
                metadata.push(packet.clone());
            } else if let Some(payload) = packet.payload() {
                carried
                    .entry(payload.input_set_id())
                    .or_default()
                    .push(payload.clone());
            }
        }
    }
    let empty = |volumes| Read {
        sets: Vec::new(),
        payloads: Vec::new(),
        volumes,
    };
    let Ok(sets) = Par3Set::from_packets(metadata) else {
        return empty(volumes);
    };
    let described: BTreeSet<InputSetId> = sets.iter().map(Par3Set::input_set_id).collect();
    if sets.len() > max_sets || carried.keys().any(|id| !described.contains(id)) {
        return empty(volumes);
    }
    let payloads = sets
        .iter()
        .map(|set| payloads(set, carried.remove(&set.input_set_id()).unwrap_or_default()))
        .collect();
    Read {
        sets,
        payloads,
        volumes,
    }
}

// What the set carries of `found`, its data and recovery packets in the
// order found: data blocks as they are, and recovery blocks under its own
// root, by index then matrix, with every block left out that shares its
// row of its matrix with another.
fn payloads(set: &Par3Set, found: Vec<PayloadRef>) -> Payloads {
    let matrices: HashSet<Fingerprint> = set.matrix_packets().iter().map(Packet::hash).collect();
    let mut rows = Vec::new();
    let mut data = Vec::new();
    for payload in found {
        match payload.kind() {
            PayloadKind::Data { index } => data.push((index, payload)),
            PayloadKind::Recovery {
                root,
                matrix,
                index,
            } if root == set.root_hash() => rows.push(Row {
                index,
                matrix,
                present: matrices.contains(&matrix),
                payload,
            }),
            PayloadKind::Recovery { .. } => {}
        }
    }
    rows.sort_by_key(|row| (row.index, row.matrix));
    let mut recovery: Vec<Row> = Vec::with_capacity(rows.len());
    // How many rows at the end of `recovery` share one key.
    let mut run = 0;
    for row in rows {
        let same = recovery
            .last()
            .is_some_and(|last| (last.index, last.matrix) == (row.index, row.matrix));
        if !same {
            if run > 1 {
                recovery.truncate(recovery.len() - run);
            }
            run = 0;
        }
        recovery.push(row);
        run += 1;
    }
    if run > 1 {
        recovery.truncate(recovery.len() - run);
    }
    Payloads { recovery, data }
}
