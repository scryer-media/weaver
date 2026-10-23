// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A pretend engine for tests: every volume is an eight-byte header and a
// body, and a run echoes each body, read through its volumes in order, as a
// member of its own named `echo-<volume>.bin`. Its first volume carries a
// store's signature so the sniffer names it a store, which the echoes build
// as this engine; the volumes after it are claimed by their `.partN`
// names. It bids for the volumes after the one its run reached, nearest
// first, and logs the bids it made.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use weaver_graph::{
    Builder, ByteRange, Delivery, Emit, Engine, Extent, Extraction, Kind, MemberId, Object,
    ObjectId, Placement, Provenance, Pull, Seek, Sink, VolumeIndex, Volumes, Weight, Why,
};

pub(crate) const SIGNATURE: &[u8] = b"Rar!\x1a\x07\x01\x00";
pub(crate) const HEADER: usize = SIGNATURE.len();
const CHUNK: usize = 16;

#[derive(Clone, Default)]
pub(crate) struct Echoes {
    // How many volumes each run reads.
    pub(crate) volumes: u32,
    pub(crate) bids: Arc<Mutex<Vec<Seek>>>,
    // The window each engine declares it holds resident while it runs.
    pub(crate) window: u64,
}

impl Builder for Echoes {
    // Every other kind is the real objects' to build, so a PAR2 set beside
    // the echo volumes still recovers them.
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        if !matches!(kind, Kind::RarStore | Kind::RarEngine) {
            return weaver_objects::Objects.build(kind, id);
        }
        Some(Box::new(Echo {
            volumes: self.volumes,
            bids: Arc::clone(&self.bids),
            window: self.window,
        }))
    }

    fn read(
        &self,
        _: &weaver_graph::Tree,
        _: weaver_graph::NodeId,
        _: weaver_graph::VolumeIndex,
        _: &weaver_graph::Placement,
        _: &mut Vec<u8>,
    ) -> bool {
        false
    }
}

struct Echo {
    volumes: u32,
    bids: Arc<Mutex<Vec<Seek>>>,
    window: u64,
}

impl Object for Echo {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: weaver_graph::VolumeIndex, _: weaver_graph::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarEngine
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) {}
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn claim(&mut self, name: &str, _: &[u8], _length: Option<u64>) -> Option<VolumeIndex> {
        let (_, part) = name.rsplit_once(".part")?;
        let part: u32 = part.strip_suffix(".rar")?.parse().ok()?;
        (part >= 2).then(|| VolumeIndex(part - 1))
    }
    fn reads_in_order(&self) -> bool {
        true
    }
    fn engine(&self) -> Option<&dyn Engine> {
        Some(self)
    }
    fn engine_mut(&mut self) -> Option<&mut dyn Engine> {
        Some(self)
    }
}

impl Engine for Echo {
    fn start(&mut self) -> Box<dyn Extraction> {
        Box::new(EchoRun {
            volumes: self.volumes,
        })
    }
    fn bid(&self, reached: VolumeIndex, out: &mut Vec<Seek>) {
        for (weight, volume) in (reached.0 + 1..self.volumes).enumerate() {
            let seek = Seek::data(
                VolumeIndex(volume),
                ByteRange::new(0, u64::MAX),
                Weight(weight as u32 + 1),
            );
            self.bids.lock().unwrap().push(seek);
            out.push(seek);
        }
    }
    fn reserve(&self) -> Option<u64> {
        Some(self.window)
    }
}

struct EchoRun {
    volumes: u32,
}

impl Extraction for EchoRun {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>) {
        for index in 0..self.volumes {
            let (volume, member) = (VolumeIndex(index), MemberId(index));
            let announced = Emit::Member {
                member,
                name: format!("echo-{index}.bin"),
                length: None,
            };
            if !sink(announced) {
                return;
            }
            let mut offset = HEADER as u64;
            let mut chunk = [0u8; CHUNK];
            loop {
                let read = match volumes.read_at(volume, offset, &mut chunk) {
                    Ok(read) => read,
                    Err(_) => {
                        sink(Emit::Refuse(Why::Unrepairable));
                        return;
                    }
                };
                let bytes = Emit::Bytes {
                    member,
                    offset: offset - HEADER as u64,
                    bytes: Bytes::copy_from_slice(&chunk[..read]),
                    provenance: Provenance::Fresh,
                };
                if read > 0 && !sink(bytes) {
                    return;
                }
                offset += read as u64;
                if read < CHUNK {
                    break;
                }
            }
            if !sink(Emit::MemberClosed(member)) {
                return;
            }
        }
    }
}
