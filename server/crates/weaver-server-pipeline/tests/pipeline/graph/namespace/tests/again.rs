// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An object whose request for bytes again may wait asks again while its
// bytes are being said again: the request is answered after that replay,
// never inside it, however many follow one another.

use super::*;

const ASKS: u32 = 64;
const LENGTH: u64 = 40;
const FIRST: ByteRange = ByteRange { start: 0, end: 10 };
const SECOND: ByteRange = ByteRange { start: 20, end: 30 };

// A pretend store whose volume is all envelope. Each time bytes from its
// front come, up to `ASKS` times, it asks for two runs of its volume again;
// the next ask comes from the first run's replay, before the second run
// has been said.
struct Chain {
    given: Vec<Option<u8>>,
    asks: u32,
    waits: bool,
    heard: Arc<Mutex<Vec<u64>>>,
}

impl Object for Chain {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
    }

    fn again_waits(&self) -> bool {
        self.waits
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        self.heard.lock().expect("heard").push(delivery.offset);
        let end = delivery.offset as usize + delivery.bytes.len();
        if self.given.len() < end {
            self.given.resize(end, None);
        }
        for (at, byte) in delivery.bytes.iter().enumerate() {
            self.given[delivery.offset as usize + at] = Some(*byte);
        }
        if delivery.offset == 0 && self.asks < ASKS {
            self.asks += 1;
            for range in [FIRST, SECOND] {
                out.push(Emit::Again {
                    volume: delivery.volume,
                    range,
                });
            }
        }
    }

    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope { envelope: 0, range });
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }

    fn posted(
        &self,
        _: VolumeIndex,
        range: ByteRange,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        let Some(bytes) = self.given.get(range.start as usize..range.end as usize) else {
            return false;
        };
        let bytes: Option<Vec<u8>> = bytes.iter().copied().collect();
        bytes.is_some_and(|bytes| {
            into.extend_from_slice(&bytes);
            true
        })
    }
}

struct Chains {
    waits: bool,
    heard: Arc<Mutex<Vec<u64>>>,
}

impl Builder for Chains {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| {
            Box::new(Chain {
                given: Vec::new(),
                asks: 0,
                waits: self.waits,
                heard: Arc::clone(&self.heard),
            }) as Box<dyn Object>
        })
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

// The store's volume delivered whole: what the store heard, in order, and
// the most requests the writer answered at once.
fn chained(waits: bool) -> (Vec<u64>, u32) {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut writer = NamespaceWriter::new(
        1,
        dir.path().to_path_buf(),
        Arc::new(HoldsAccountant::new(PLENTY)),
        CapacityReader::unknown(),
    );
    let mut tree = Tree::new();
    let heard = Arc::new(Mutex::new(Vec::new()));
    let mut build = Chains {
        waits,
        heard: Arc::clone(&heard),
    };
    let mut out = Vec::new();
    let member = tree.announce_root("chain.rar", Some(LENGTH), &mut Vec::new());
    writer
        .announced(
            &mut tree,
            &mut build,
            &Source,
            NodeId::ROOT,
            member,
            &mut out,
        )
        .expect("announce");
    let mut volume = SIGNATURE.to_vec();
    volume.resize(LENGTH as usize, 0x5a);
    let arrival = Arrival {
        node: NodeId::ROOT,
        member,
        offset: 0,
        bytes: Bytes::copy_from_slice(&volume).into(),
        provenance: Provenance::Fresh,
    };
    writer
        .deliver(&mut tree, &mut build, &Source, arrival, &mut out)
        .expect("deliver");
    assert!(tree.child_of(NodeId::ROOT, member).is_some(), "bound");
    let heard = heard.lock().expect("heard").clone();
    (heard, writer.answering.1)
}

// Each request raised by the first run of a replay is answered once the
// replay's second run has been said: one request answered at a time.
#[test]
fn a_replay_asked_for_during_a_replay_runs_after_it() {
    let (heard, most) = chained(true);
    let mut expected = vec![0];
    for _ in 0..ASKS {
        expected.extend([FIRST.start, SECOND.start]);
    }
    assert_eq!(heard, expected);
    assert_eq!(most, 1, "no request is answered inside another");
}

// An object whose requests may not wait has each answered inside the
// replay that raised it, as before: what the inner one says again the
// outer one does not say a second time.
#[test]
fn a_replay_asked_for_during_a_replay_that_may_not_wait_runs_inside_it() {
    let (heard, most) = chained(false);
    let mut expected = vec![0];
    expected.extend(std::iter::repeat_n(FIRST.start, ASKS as usize));
    expected.push(SECOND.start);
    assert_eq!(heard, expected);
    assert_eq!(most, ASKS);
}
