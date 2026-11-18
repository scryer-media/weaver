// SPDX-License-Identifier: GPL-3.0-or-later

//! A replay of scripted swarms against the piece tracker, compared line by
//! line with the trace the tracker gave when it was recorded: every pick,
//! steal, doubled block, arrival and its cancels, mark, failed write,
//! release, snub hand-back and what each peer is to request. A change to
//! where the tracker keeps its state must give the same trace.
//!
//! The swarms are drawn from seeded generators, and the clock is paused and
//! moved a millisecond a step, so a replay depends on nothing but the
//! seeds. A peer owns one piece at most at a time, as the order in which a
//! peer's several pieces leave flight follows a hash map's order, which
//! the trace must not depend on.
//!
//! `WEAVER_BLESS_GOLDEN=1` writes the trace anew instead of comparing it.

use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use super::*;
use crate::core::constants::CHUNK_SIZE;
use crate::core::lengths::Lengths;
use crate::{bitv::BitV as _, type_aliases::BF};

pub(super) const PIECES: u32 = 10;
pub(super) const CHUNKS: u32 = 4;
const PEERS: u8 = 4;
const STEPS: usize = 500;
const SEEDS: [u64; 4] = [1, 7, 42, 1009];
const GOLDEN: &str = include_str!("golden_trace.txt");

pub(super) fn peer(id: u8) -> PeerHandle {
    (SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, id)), 6881)).into()
}

fn name(handle: PeerHandle) -> String {
    (1..=PEERS)
        .find(|id| peer(*id) == handle)
        .map_or_else(|| format!("{handle:?}"), |id| format!("p{id}"))
}

fn names(handles: &[PeerHandle]) -> String {
    handles
        .iter()
        .map(|h| name(*h))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn tracker(seed: u64) -> PieceTracker {
    let total_length = u64::from(CHUNK_SIZE * CHUNKS) * u64::from(PIECES);
    let lengths = Lengths::new(total_length, CHUNK_SIZE * CHUNKS).unwrap();
    let have = BF::from_boxed_slice(vec![0u8; lengths.piece_bitfield_bytes()].into_boxed_slice());
    let file_infos = vec![crate::file_info::FileInfo {
        relative_filename: "golden.dat".into(),
        offset_in_torrent: 0,
        len: total_length,
        piece_range: 0..PIECES,
        attrs: Default::default(),
    }];
    let selected = crate::chunk_tracker::compute_selected_bytes(&lengths, |_| true, &file_infos);
    let chunks = ChunkTracker::new(have.into_dyn(), selected, lengths, &file_infos).unwrap();
    PieceTracker::with_rng(chunks, StdRng::seed_from_u64(seed))
}

pub(super) fn piece_of(t: &PieceTracker, index: u32) -> ValidPieceIndex {
    t.chunks()
        .get_lengths()
        .validate_piece_index(index)
        .unwrap()
}

pub(super) fn chunk_of(t: &PieceTracker, index: u32, chunk: u32) -> ChunkInfo {
    let lengths = t.chunks().get_lengths();
    lengths
        .chunk_info_from_received_data(piece_of(t, index), chunk * CHUNK_SIZE, CHUNK_SIZE)
        .unwrap()
}

fn block(c: &ChunkInfo) -> String {
    format!("{}.{}", c.piece_index.get(), c.chunk_index)
}

// The seams the replay drives the tracker through: a block arrives, its
// write ends, a piece is taken for its check. Each is what the torrent does
// for one block or piece.

fn arrive(
    t: &mut PieceTracker,
    c: &ChunkInfo,
    sender: PeerHandle,
    room: bool,
    asked: &mut Vec<(u32, bool)>,
) -> Arrival {
    // A piece in flight rules under its own lock, as the torrent does;
    // any other, the tracker under the torrent's.
    let mut admit = |p: ValidPieceIndex, owned| {
        asked.push((p.get(), owned));
        room
    };
    let slots = t.slots().clone();
    match slots.arrive(c, sender, &mut admit) {
        Some(arrival) => arrival,
        None => t.admitted_block_arrived(c, sender, admit),
    }
}

fn mark(t: &mut PieceTracker, c: &ChunkInfo, sender: PeerHandle) -> Option<ChunkMarkingResult> {
    if t.slots().mark(c, sender) {
        return Some(ChunkMarkingResult::NotCompleted);
    }
    let data = vec![0u8; CHUNK_SIZE as usize];
    t.mark_chunk_downloaded(
        &Piece::from_data(c.piece_index.get(), c.offset, &data[..]),
        sender,
    )
}

fn write_failed(t: &mut PieceTracker, c: &ChunkInfo) {
    if !t.slots().write_failed(c) {
        t.write_failed(c);
    }
}

fn take(t: &mut PieceTracker, p: ValidPieceIndex) -> (Option<Duration>, Senders) {
    t.take_inflight(p)
}

struct Swarm {
    t: PieceTracker,
    rng: StdRng,
    // Which pieces each peer has.
    has: Vec<Vec<bool>>,
    // What each peer was asked for and has not sent.
    asked: Vec<Vec<ChunkInfo>>,
    // Blocks admitted and being written, and by whom.
    writing: Vec<(PeerHandle, ChunkInfo)>,
    // Pieces out for their check.
    checking: Vec<ValidPieceIndex>,
    out: String,
}

impl Swarm {
    fn new(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed ^ 0x5eed);
        let has = (0..PEERS)
            .map(|_| (0..PIECES).map(|_| rng.random_range(0..4) != 0).collect())
            .collect();
        Self {
            t: tracker(seed),
            rng,
            has,
            asked: vec![Vec::new(); PEERS as usize],
            writing: Vec::new(),
            checking: Vec::new(),
            out: String::new(),
        }
    }

    fn owned(&self, who: PeerHandle) -> Option<u32> {
        (0..PIECES).find(|p| self.t.is_owned_by(piece_of(&self.t, *p), who))
    }

    fn log(&mut self, line: String) {
        self.out.push_str(&line);
        self.out.push('\n');
    }

    fn step(&mut self) {
        let id = self.rng.random_range(1..=PEERS);
        let who = peer(id);
        match self.rng.random_range(0..100) {
            0..30 => self.acquire(id),
            30..62 => self.deliver(id),
            62..80 => self.finish_write(),
            80..85 => self.check(),
            85..89 => {
                let n = self.t.release_pieces_owned_by(who);
                self.asked[usize::from(id - 1)].clear();
                let requeued = self.t.take_requeued();
                self.log(format!(
                    "gone {} released={n} requeued={:?}",
                    name(who),
                    requeued.iter().map(|p| p.get()).collect::<Vec<_>>()
                ));
            }
            89..94 => {
                let asked = self.asked[usize::from(id - 1)].clone();
                let released = self
                    .t
                    .release_piece_blocked_by(who, |c| asked.contains(c))
                    .map(|p| p.get());
                let requeued = self.t.take_requeued();
                self.log(format!(
                    "snub {} released={released:?} requeued={:?}",
                    name(who),
                    requeued.iter().map(|p| p.get()).collect::<Vec<_>>()
                ));
            }
            94..97 => {
                let n = self.t.release_started_pieces_owned_by(who);
                let requeued = self.t.take_requeued();
                self.log(format!(
                    "room_wait {} released={n} requeued={:?}",
                    name(who),
                    requeued.iter().map(|p| p.get()).collect::<Vec<_>>()
                ));
            }
            _ => self.look(id),
        }
    }

    fn acquire(&mut self, id: u8) {
        let who = peer(id);
        let i = usize::from(id - 1);
        // A peer owns one piece at a time.
        if self.owned(who).is_some() {
            self.look(id);
            return;
        }
        // Mostly no piece time, so the end game doubles blocks as well as
        // takes pieces over.
        let avg = match self.rng.random_range(0..5) {
            0..3 => None,
            n => Some(Duration::from_millis(n - 2)),
        };
        let reverse = self.rng.random_range(0..5) == 0;
        let on_parole = self.rng.random_range(0..8) == 0;
        let admit = self.rng.random_range(0..6) != 0;
        let outstanding = self.asked[i].len();
        let has = self.has[i].clone();
        let has_all = self.has.clone();
        let writing: Vec<u32> = self
            .writing
            .iter()
            .map(|(_, c)| c.piece_index.get())
            .collect();
        let mut admitted = Vec::new();
        let result = self.t.acquire_admitted_piece(
            AcquireRequest {
                peer: who,
                peer_avg_time: avg,
                priority_pieces: std::iter::empty(),
                peer_has_piece: |p: ValidPieceIndex| has[p.get_usize()],
                availability: |p: ValidPieceIndex| {
                    u32::try_from(has_all.iter().filter(|h| h[p.get_usize()]).count())
                        .expect("few peers")
                },
                moved: Moved {
                    pieces: Vec::new(),
                    all: true,
                },
                num_peers: usize::from(PEERS),
                outstanding_requests: outstanding,
                options: PickOptions {
                    reverse,
                    on_parole,
                    suggested: &[],
                    sequential: false,
                },
                // As the torrent does: a piece with a block being written
                // is not taken over.
                can_steal: |p: ValidPieceIndex| !writing.contains(&p.get()),
            },
            |p| {
                admitted.push(p.get());
                admit
            },
        );
        let piece = match &result {
            AcquireResult::Reserved(p)
            | AcquireResult::Stolen { piece: p, .. }
            | AcquireResult::EndGame { piece: p, .. } => Some(*p),
            AcquireResult::NoneAvailable | AcquireResult::NoRoom => None,
        };
        let shown = match &result {
            AcquireResult::Reserved(p) => format!("reserved {}", p.get()),
            AcquireResult::Stolen { piece, from_peer } => {
                // The peer it was taken from stops waiting for its blocks.
                let from = usize::from((1..=PEERS).find(|id| peer(*id) == *from_peer).unwrap() - 1);
                self.asked[from].retain(|c| c.piece_index != *piece);
                format!("stolen {} from {}", piece.get(), name(*from_peer))
            }
            AcquireResult::EndGame { piece, chunk } => {
                format!("doubled {} {}", piece.get(), block(chunk))
            }
            AcquireResult::NoneAvailable => "none".to_owned(),
            AcquireResult::NoRoom => "no_room".to_owned(),
        };
        let mut line = format!(
            "acquire {} avg={avg:?} rev={reverse} parole={on_parole} out={outstanding} admit={admitted:?} -> {shown}",
            name(who)
        );
        if let Some(p) = piece {
            let to_ask: Vec<ChunkInfo> = self.t.chunks_to_request(p, who).collect();
            let _ = write!(
                line,
                " ask=[{}]",
                to_ask.iter().map(block).collect::<Vec<_>>().join(" ")
            );
            for c in to_ask {
                // Asked once: a block asked already is not asked again.
                if self.t.may_request(p, who) && !self.asked[i].contains(&c) {
                    self.asked[i].push(c);
                }
            }
        }
        self.log(line);
    }

    fn deliver(&mut self, id: u8) {
        let who = peer(id);
        let i = usize::from(id - 1);
        // Mostly a block asked for, now and then one never asked for.
        let c = if !self.asked[i].is_empty() && self.rng.random_range(0..6) != 0 {
            let at = self.rng.random_range(0..self.asked[i].len());
            self.asked[i].remove(at)
        } else {
            let p = self.rng.random_range(0..PIECES);
            let c = self.rng.random_range(0..CHUNKS);
            if !self.has[i][p as usize] {
                return self.look(id);
            }
            chunk_of(&self.t, p, c)
        };
        let room = self.rng.random_range(0..8) != 0;
        let mut asked = Vec::new();
        let arrival = arrive(&mut self.t, &c, who, room, &mut asked);
        let shown = match &arrival {
            Arrival::Write { cancel } => {
                for other in cancel {
                    let o = usize::from((1..=PEERS).find(|id| peer(*id) == *other).unwrap() - 1);
                    self.asked[o].retain(|a| a != &c);
                }
                self.writing.push((who, c));
                format!("write cancel=[{}]", names(cancel))
            }
            Arrival::Discard => "discard".to_owned(),
            Arrival::NoRoom => "no_room".to_owned(),
        };
        self.log(format!(
            "arrive {} {} room={room} asked={asked:?} -> {shown}",
            name(who),
            block(&c)
        ));
    }

    fn finish_write(&mut self) {
        if self.writing.is_empty() {
            return self.log("idle".to_owned());
        }
        let at = self.rng.random_range(0..self.writing.len());
        let (who, c) = self.writing.remove(at);
        if self.rng.random_range(0..10) == 0 {
            write_failed(&mut self.t, &c);
            return self.log(format!("write_failed {} {}", name(who), block(&c)));
        }
        let result = mark(&mut self.t, &c, who);
        let mut line = format!("mark {} {} -> {result:?}", name(who), block(&c));
        if let Some(ChunkMarkingResult::Completed) = result {
            let (time, senders) = take(&mut self.t, c.piece_index);
            let _ = write!(
                line,
                " take time={time:?} senders=[{}]",
                senders
                    .blocks()
                    .iter()
                    .map(|s| s.map_or_else(|| "-".to_owned(), name))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            self.checking.push(c.piece_index);
        }
        self.log(line);
    }

    fn check(&mut self) {
        if self.checking.is_empty() {
            return self.log("idle".to_owned());
        }
        let p = self.checking.remove(0);
        let passed = self.rng.random_range(0..5) != 0;
        if passed {
            self.t.mark_piece_hash_ok(p);
        } else {
            self.t.mark_piece_hash_failed(p);
        }
        self.log(format!("check {} passed={passed}", p.get()));
    }

    // What the peer would request and whether it may, for every piece.
    fn look(&mut self, id: u8) {
        let who = peer(id);
        let mut line = format!("look {}", name(who));
        for p in 0..PIECES {
            let piece = piece_of(&self.t, p);
            let to_ask: Vec<String> = self
                .t
                .chunks_to_request(piece, who)
                .map(|c| block(&c))
                .collect();
            if self.t.may_request(piece, who) {
                let _ = write!(line, " {p}:[{}]", to_ask.join(" "));
            }
        }
        self.log(line);
    }

    fn state(&mut self) {
        let t = &self.t;
        let mut line = String::from("state");
        let queued: Vec<u32> = t.chunks().iter_queued_pieces().map(|p| p.get()).collect();
        let _ = write!(
            line,
            " queued={queued:?} end_game={}",
            t.chunks().every_queued_piece_started()
        );
        for p in 0..PIECES {
            let piece = piece_of(t, p);
            let owner = (1..=PEERS)
                .find(|id| t.is_owned_by(piece, peer(*id)))
                .map_or_else(|| "-".to_owned(), |id| format!("p{id}"));
            let missing: Vec<u32> = t.missing_chunks(piece).map(|c| c.chunk_index).collect();
            let _ = write!(
                line,
                " {p}:{}{owner}{missing:?}",
                if t.chunks().is_piece_have(piece) {
                    "H"
                } else {
                    ""
                }
            );
        }
        self.log(line);
    }
}

async fn replay(seed: u64) -> String {
    let mut swarm = Swarm::new(seed);
    let _ = writeln!(swarm.out, "seed {seed}");
    for step in 0..STEPS {
        tokio::time::advance(Duration::from_millis(1)).await;
        swarm.step();
        // A piece holds a slot while in flight, and none otherwise.
        assert_eq!(swarm.t.slots().held(), swarm.t.inflight_count());
        if step % 20 == 19 {
            swarm.state();
        }
    }
    swarm.state();
    swarm.out
}

#[tokio::test(start_paused = true)]
async fn the_tracker_replays_its_recorded_swarms_exactly() {
    let mut trace = String::new();
    for seed in SEEDS {
        trace.push_str(&replay(seed).await);
    }
    if std::env::var_os("WEAVER_BLESS_GOLDEN").is_some() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/piece_tracker/golden_trace.txt"
        );
        std::fs::write(path, &trace).unwrap();
        return;
    }
    for (n, (got, want)) in trace.lines().zip(GOLDEN.lines()).enumerate() {
        assert_eq!(got, want, "line {} of the trace", n + 1);
    }
    assert_eq!(trace.lines().count(), GOLDEN.lines().count());
}
