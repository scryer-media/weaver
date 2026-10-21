// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Pretend objects for the refusal and password paths. Every volume is an
// eight-byte header and a body. A store keeps the header as its envelope and
// the body as a member of its own, `inner-<volume>.bin`, then gives up on
// its last volume as its mode says; a locked store keeps everything until a
// password opens it; an engine reads its first volume's body as
// `unlocked.bin`, or refuses it.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use weaver_graph::{
    Builder, ByteRange, Delivery, Emit, Engine, Extent, Extraction, Kind, MemberId, Object,
    ObjectId, Placement, Pull, Seek, Sink, Standing, VolumeIndex, Volumes, Why,
};

use crate::pipeline::graph::engine::echo::{Echoes, HEADER};

pub(super) const OPENS: &str = "lantern-three";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode {
    // The store refuses once its last volume closed.
    Refuse,
    // The store asks for an engine once its last volume closed; the echo
    // engine is built for it.
    NeedsEngine,
    // The store places nothing and refuses once its last volume closed.
    Hold,
    // The store's body is locked until `OPENS` is offered.
    Locked,
    // The store is built as an engine whose run needs `OPENS`.
    LockedEngine,
    // The store is built as an engine whose run refuses.
    RefusingEngine,
}

#[derive(Clone)]
pub(super) struct Fakes {
    pub(super) mode: Mode,
    pub(super) volumes: u32,
    pub(super) offers: Arc<Mutex<Vec<String>>>,
    pub(super) starts: Arc<AtomicUsize>,
}

impl Builder for Fakes {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        match (kind, self.mode) {
            (Kind::RarStore, Mode::LockedEngine | Mode::RefusingEngine) => {
                Some(Box::new(LockedEngine {
                    refuses: self.mode == Mode::RefusingEngine,
                    password: None,
                    offers: Arc::clone(&self.offers),
                    starts: Arc::clone(&self.starts),
                }))
            }
            (Kind::RarStore, mode) => Some(Box::new(Store {
                mode,
                volumes: self.volumes,
                announced: BTreeSet::new(),
                body: Vec::new(),
                asked: false,
                opened: false,
                offers: Arc::clone(&self.offers),
            })),
            (Kind::RarEngine, Mode::NeedsEngine) => Echoes {
                volumes: self.volumes,
                ..Echoes::default()
            }
            .build(kind, id),
            _ => None,
        }
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

struct Store {
    mode: Mode,
    volumes: u32,
    announced: BTreeSet<u32>,
    body: Vec<u8>,
    asked: bool,
    opened: bool,
    offers: Arc<Mutex<Vec<String>>>,
}

impl Store {
    // Whether the body is the store's member: not while it is locked or
    // places nothing.
    fn streams(&self) -> bool {
        match self.mode {
            Mode::Locked => self.opened,
            Mode::Hold => false,
            _ => true,
        }
    }
}

impl Object for Store {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: weaver_graph::VolumeIndex, _: weaver_graph::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        let header = HEADER as u64;
        let end = delivery.offset + delivery.bytes.len() as u64;
        if end <= header {
            return;
        }
        let start = delivery.offset.max(header);
        let body = &delivery.bytes[(start - delivery.offset) as usize..];
        match self.mode {
            Mode::Hold => {}
            Mode::Locked => {
                self.body.extend_from_slice(body);
                if !self.asked {
                    self.asked = true;
                    out.push(Emit::NeedsPassword);
                }
            }
            _ => {
                let member = MemberId(delivery.volume.0);
                if self.announced.insert(delivery.volume.0) {
                    out.push(Emit::Member {
                        member,
                        name: format!("inner-{}.bin", delivery.volume.0),
                        length: None,
                    });
                }
                out.push(Emit::Bytes {
                    member,
                    offset: start - header,
                    bytes: Bytes::copy_from_slice(body),
                    provenance: delivery.provenance,
                });
            }
        }
    }

    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        if self.announced.contains(&volume.0) {
            out.push(Emit::MemberClosed(MemberId(volume.0)));
        }
        if volume.0 + 1 < self.volumes {
            return;
        }
        match self.mode {
            Mode::Refuse | Mode::Hold => out.push(Emit::Refuse(Why::Unsupported)),
            Mode::NeedsEngine => out.push(Emit::NeedsEngine(Why::Compressed)),
            _ => {}
        }
    }

    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}

    // A locked store still asking once nothing more will come refuses
    // itself: its header stays shut.
    fn quiet(&mut self, _: &dyn Standing, out: &mut Vec<Emit>) {
        if self.mode == Mode::Locked && self.asked && !self.opened {
            out.push(Emit::Refuse(Why::EncryptedHeader));
        }
    }

    fn wants(&mut self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let header = HEADER as u64;
        if !self.streams() {
            out.push(match self.mode {
                Mode::Hold => Placement::Held { range },
                _ => Placement::Envelope {
                    envelope: volume.0,
                    range,
                },
            });
            return;
        }
        if range.start < header {
            out.push(Placement::Envelope {
                envelope: volume.0,
                range: ByteRange::new(range.start, range.end.min(header)),
            });
        }
        if range.end > header {
            out.push(Placement::Member {
                member: MemberId(volume.0),
                range: ByteRange::new(range.start.max(header) - header, range.end - header),
            });
        }
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) {}

    fn offer_password(&mut self, secret: &str, out: &mut Vec<Emit>) {
        self.offers.lock().unwrap().push(secret.to_string());
        if secret != OPENS {
            out.push(Emit::NeedsPassword);
            return;
        }
        self.opened = true;
        let member = MemberId(0);
        out.push(Emit::Member {
            member,
            name: "opened.bin".to_string(),
            length: Some(self.body.len() as u64),
        });
        out.push(Emit::Bytes {
            member,
            offset: 0,
            bytes: Bytes::copy_from_slice(&self.body),
            provenance: weaver_graph::Provenance::Fresh,
        });
        out.push(Emit::MemberClosed(member));
    }

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }

    fn claim(&mut self, name: &str, _: &[u8], _length: Option<u64>) -> Option<VolumeIndex> {
        let (_, part) = name.rsplit_once(".part")?;
        let part: u32 = part.strip_suffix(".rar")?.parse().ok()?;
        (part >= 2).then(|| VolumeIndex(part - 1))
    }
}

// An engine over one volume: its run reads the body once `OPENS` opened it,
// asks for a password before, or refuses outright.
struct LockedEngine {
    refuses: bool,
    password: Option<String>,
    offers: Arc<Mutex<Vec<String>>>,
    starts: Arc<AtomicUsize>,
}

impl Object for LockedEngine {
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
    fn wants(&mut self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) {}
    fn offer_password(&mut self, secret: &str, _: &mut Vec<Emit>) {
        self.offers.lock().unwrap().push(secret.to_string());
        self.password = Some(secret.to_string());
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
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

impl Engine for LockedEngine {
    fn start(&mut self) -> Box<dyn Extraction> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Box::new(LockedRun {
            refuses: self.refuses,
            opened: self.password.as_deref() == Some(OPENS),
        })
    }
}

struct LockedRun {
    refuses: bool,
    opened: bool,
}

impl Extraction for LockedRun {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>) {
        if self.refuses {
            sink(Emit::Refuse(Why::Unsupported));
            return;
        }
        if !self.opened {
            sink(Emit::NeedsPassword);
            return;
        }
        let member = MemberId(0);
        let announced = Emit::Member {
            member,
            name: "unlocked.bin".to_string(),
            length: None,
        };
        if !sink(announced) {
            return;
        }
        let mut offset = HEADER as u64;
        let mut chunk = [0u8; 16];
        loop {
            let Ok(read) = volumes.read_at(VolumeIndex(0), offset, &mut chunk) else {
                sink(Emit::Refuse(Why::Unrepairable));
                return;
            };
            if read == 0 {
                break;
            }
            let bytes = Emit::Bytes {
                member,
                offset: offset - HEADER as u64,
                bytes: Bytes::copy_from_slice(&chunk[..read]),
                provenance: weaver_graph::Provenance::Fresh,
            };
            if !sink(bytes) {
                return;
            }
            offset += read as u64;
        }
        sink(Emit::MemberClosed(member));
    }
}
