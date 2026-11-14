// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// yEnc decode bench: each weaver decoder tier against the matching rapidyenc
// kernel (forced), same process, interleaved samples. The decode
// counterpart of encode-bench, with the same method.
//
//   WEAVER_RAPIDYENC_SRC=/path/to/rapidyenc \
//     cargo run --locked --release -- [--quick] [--tier NAME] [--fixture NAME] [--mode raw|end]
//                                      [--verify] [--hash-fixtures] [--perf weaver|rapidyenc|none]
//
// Two modes per fixture: `raw` is weaver's `decode_rapidyenc` against
// rapidyenc's raw `decode` (dot-unstuffing, no end search); `end` is
// `decode_rapidyenc_incremental` against `decode_end` (end search). Each
// input is a yEnc body (128 columns, CRLF) followed by "\r\n=yend\r\n".
// Before timing, both sides' output bytes, consumed and written counts and
// end codes are checked equal. `ratio` is rapidyenc time / weaver time:
// above 1 means weaver is faster.
//
// Buffers sit at fixed page offsets on both sides; every function on both
// sides starts on a 64-byte boundary (.cargo/config.toml, build.rs). Run it
// under `setarch -R`; the `aslr:` header line says whether that was done.
// No timing assertions anywhere.
//
// `random` is rapidyenc tool/bench.cc's one shape: one 768,000-byte random
// article, decoded again and again from the same buffer; `random-716k` is
// the same at the captured post's part size.
//
// The `real-` fixtures feed the decoder as a download connection does (see
// `run_real`): one 768,000-byte part per NNTP BODY transcript, the transcript
// delivered in socket reads of a fixed size into a reused read buffer, each
// read copied there untimed just before the timed decode calls, the decoder
// state and `line=128` hint carried across calls, and each article decoded
// into its own output region. They run in `end` mode only, and time ns per
// article summed over its decode calls.

use std::hint::black_box;
use std::time::Instant;

use weaver_yenc::simd::bench::{
    ChunkFn, ChunkState, DecodeFn, TIERS, Tier, resolve, resolve_chunk, runs, unpack,
};

const LINE_SIZE: usize = 128;
const DST_PAGE_OFFSET: usize = 2048;
const TRAILER: &[u8] = b"\r\n=yend\r\n";

struct Paged {
    buf: Vec<u8>,
    start: usize,
    len: usize,
}

impl Paged {
    fn new(len: usize, off: usize) -> Self {
        let buf = vec![0u8; len + 8192];
        let start = off.wrapping_sub(buf.as_ptr() as usize) & 4095;
        Paged { buf, start, len }
    }
    fn bytes(&mut self) -> &mut [u8] {
        &mut self.buf[self.start..self.start + self.len]
    }
}

unsafe extern "C" {
    fn weaver_rapidyenc_decode_init();
    fn weaver_rapidyenc_decode_crlf(
        src: *const core::ffi::c_void,
        dest: *mut core::ffi::c_void,
        len: u64,
    ) -> u64;
    fn weaver_rapidyenc_decode_end(
        src: *const core::ffi::c_void,
        dest: *mut core::ffi::c_void,
        len: u64,
        consumed: *mut u64,
        written: *mut u64,
    ) -> i32;
    fn weaver_rapidyenc_decode_end_state(
        src: *const core::ffi::c_void,
        dest: *mut core::ffi::c_void,
        len: u64,
        consumed: *mut u64,
        written: *mut u64,
        state: *mut i32,
    ) -> i32;
    fn weaver_rapidyenc_decode_kernel() -> i32;
    fn weaver_rapidyenc_decode_force(isa: i32) -> i32;
}

// Weaver tier and the rapidyenc kernel (YEncDecIsaLevel) it is measured
// against. rapidyenc has no SSE4.1 decoder; a CPU with SSE4.1 but no AVX
// runs its SSSE3 one.
fn kernel_for(tier: Tier) -> (i32, &'static str) {
    match tier {
        Tier::Scalar => (0, "generic"),
        Tier::Neon => (0x1000, "NEON"),
        Tier::Sse2 => (0x100, "SSE2"),
        Tier::Ssse3 | Tier::Sse41 => (0x200, "SSSE3"),
        Tier::Avx => (0x381, "AVX"),
        Tier::Avx2 => (0x403, "AVX2"),
        Tier::Avx512Vbmi2 => (0x603, "VBMI2"),
    }
}

struct Options {
    quick: bool,
    tier: Option<String>,
    fixture: Option<String>,
    mode: Option<String>,
    perf: Option<String>,
    verify: bool,
    hash_fixtures: bool,
}

fn parse_args() -> Options {
    let mut o = Options {
        quick: false,
        tier: None,
        fixture: None,
        mode: None,
        perf: None,
        verify: false,
        hash_fixtures: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--quick" => o.quick = true,
            "--verify" => o.verify = true,
            "--hash-fixtures" => o.hash_fixtures = true,
            "--tier" => o.tier = Some(args.next().expect("--tier needs a name")),
            "--fixture" => o.fixture = Some(args.next().expect("--fixture needs a name")),
            "--mode" => o.mode = Some(args.next().expect("--mode needs raw or end")),
            "--perf" => o.perf = Some(args.next().expect("--perf needs weaver, rapidyenc or none")),
            "-h" | "--help" => {
                println!(
                    "usage: decode-bench [--quick] [--tier NAME] [--fixture NAME] [--mode raw|end] [--perf weaver|rapidyenc|none] [--verify] [--hash-fixtures]\n\
                     tiers: {}\nfixtures: {}",
                    TIERS.map(|t| t.as_str()).join(", "),
                    FIXTURES
                        .map(|f| f.0)
                        .into_iter()
                        .chain(REAL.map(|r| r.0))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                std::process::exit(0);
            }
            other => panic!("unknown argument {other:?} (try --help)"),
        }
    }
    o
}

// ---- fixtures (decoded data as encode-bench's, then yEnc-encoded) ---------

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn random(len: usize) -> Vec<u8> {
    let mut state = 0x7e57_0000_0001;
    (0..len).map(|_| splitmix(&mut state) as u8).collect()
}

type Fixture = (&'static str, fn() -> Vec<u8>);

const FIXTURES: [Fixture; 2] = [
    ("random", || random(768_000)),
    // `random` at the size of a full part of the captured reference post.
    ("random-716k", || random(716_800)),
];

// Room for an encoded body of `len` bytes: every byte escaped, a CRLF per
// line, and slack; the formula of the encoder's `encode_max_len`.
fn encode_max_len(len: usize) -> usize {
    let doubled = len * 2;
    doubled + 2 + 64 + (doubled / LINE_SIZE) * 2
}

// One yEnc body (one byte at a time) plus the trailer, with the escaping of
// rapidyenc's generic encoder (src/encoder.cc:30-145, do_encode_generic):
// NUL, LF, CR and '=' everywhere; also TAB, space and '.' first on a line
// (escapedLUT, encoder.cc:17-21); TAB and space last on a line but not '.'
// (encoder.cc:105-113); a line ends once its column reaches LINE_SIZE and
// input remains (encoder.cc:74-129); a final TAB or space is escaped
// (encoder.cc:133-142). Its 8-byte unrolled path (encoder.cc:51-80) emits
// what this loop does, reverting itself when it would cross the line end.
fn encode_article(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(encode_max_len(data.len()));
    let mut col = 0usize;
    for (i, &c) in data.iter().enumerate() {
        let e = c.wrapping_add(42);
        let critical = matches!(e, 0 | b'\n' | b'\r' | b'=');
        let escape = if col == 0 {
            critical || matches!(e, b'\t' | b' ' | b'.')
        } else if col < LINE_SIZE - 1 {
            critical
        } else {
            critical || matches!(e, b'\t' | b' ')
        };
        if escape {
            out.push(b'=');
            out.push(e.wrapping_add(64));
            col += 2;
        } else {
            out.push(e);
            col += 1;
        }
        if col >= LINE_SIZE && i + 1 < data.len() {
            out.extend_from_slice(b"\r\n");
            col = 0;
        }
    }
    if let Some(&last) = out.last()
        && (last == b'\t' || last == b' ')
    {
        *out.last_mut().unwrap() = b'=';
        out.push(last + 64);
    }
    out.extend_from_slice(TRAILER);
    out
}

// FNV-1a 64, to compare fixtures across bench builds.
fn fnv1a(h: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        *h ^= b as u64;
        *h = h.wrapping_mul(0x0100_0000_01b3);
    }
}

// ---- real-shape fixtures --------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum RealSrc {
    // Seeded random parts of REAL_PART bytes, encoded here.
    Synth,
    // Captured bodies in WEAVER_YENC_REAL_DIR: full-size parts, or a file's
    // last (shorter) part.
    Full,
    Last,
}

// (name, source, bytes per socket read). 128 KiB is the largest socket read
// a download connection takes (weaver-nntp connection.rs, SOCKET_READ_MAX);
// 64 KiB is what that constant's comment says reads from a real provider
// rarely pass.
const REAL: [(&str, RealSrc, usize); 4] = [
    ("real-full", RealSrc::Full, 128 << 10),
    ("real-full-64k", RealSrc::Full, 64 << 10),
    ("real-last", RealSrc::Last, 128 << 10),
    ("real-synth", RealSrc::Synth, 128 << 10),
];
// A full part of the captured reference post: 716,800 decoded bytes, line=128.
const REAL_PART: usize = 716_800;
const REAL_LINE: u32 = 128;
const REAL_DIR_ENV: &str = "WEAVER_YENC_REAL_DIR";
// Each side writes a different output region per article, from a ring this
// large, so an article's output is not in cache when it starts (production
// allocates each article's output afresh; the last one is still in flight).
const REAL_OUT_RING: usize = 128 << 20;
// The connection reserves one socket read before each read, into a read
// buffer of two (NntpBufferProfile::with_socket_read, read_into).
const SOCKET_READ: usize = 128 << 10;
const READ_BUF: usize = 2 * SOCKET_READ;
// What the fused decoder hands the kernel once a header-sized batch is full
// (fused_yenc.rs, TAIL_PROBE_INPUT).
const TAIL_PROBE: usize = 256;

// NNTP BODY transcripts, one per article `stride` apart: the status line
// (which the connection reads before the fused decoder starts), the body as
// it came off the wire (`=ybegin`, `=ypart`, data, `=yend`), and the dot
// line. `hdrs` is the bytes before the first data byte, `parts` the decoded
// size `=ypart` declares, `crcs` the `=yend pcrc32`.
struct RealPool {
    pool: Paged,
    stride: usize,
    lens: Vec<usize>,
    hdrs: Vec<usize>,
    parts: Vec<usize>,
    crcs: Vec<u32>,
}

fn crc32(data: &[u8]) -> u32 {
    let mut c = weaver_yenc::crc::Crc32::new();
    c.update(data);
    c.finalize()
}

fn real_pool_of(bodies: Vec<(String, Vec<u8>)>) -> RealPool {
    let status: Vec<String> = bodies
        .iter()
        .map(|(name, _)| format!("222 0 <{name}@bench.invalid> body\r\n"))
        .collect();
    let stride = bodies
        .iter()
        .zip(&status)
        .map(|((_, b), s)| s.len() + b.len() + 3)
        .max()
        .unwrap()
        .div_ceil(4096)
        * 4096;
    let mut pool = Paged::new(bodies.len() * stride, 0);
    let mut rp = RealPool {
        pool: Paged::new(0, 0),
        stride,
        lens: vec![],
        hdrs: vec![],
        parts: vec![],
        crcs: vec![],
    };
    for (k, ((name, body), status)) in bodies.iter().zip(&status).enumerate() {
        // `=ybegin` and `=ypart` lines, then data up to "\r\n=yend".
        let line2 = body
            .iter()
            .enumerate()
            .filter(|&(_, &c)| c == b'\n')
            .nth(1)
            .map(|(i, _)| i + 1);
        let hdr = line2.unwrap_or_else(|| panic!("{name}: no =ypart line"));
        let text = String::from_utf8_lossy(&body[..hdr]);
        assert!(
            text.starts_with("=ybegin ") && text.contains("\n=ypart "),
            "{name}: not =ybegin/=ypart"
        );
        let field = |s: &str, key: &str| -> Option<String> {
            s.split([' ', '\r', '\n'])
                .find_map(|w| w.strip_prefix(key))
                .map(str::to_string)
        };
        let begin: usize = field(&text, "begin=")
            .and_then(|v| v.parse().ok())
            .expect("=ypart begin");
        let end: usize = field(&text, "end=")
            .and_then(|v| v.parse().ok())
            .expect("=ypart end");
        let yend_at = body
            .windows(7)
            .rposition(|w| w == b"\r\n=yend")
            .unwrap_or_else(|| panic!("{name}: no =yend"));
        let yend = String::from_utf8_lossy(&body[yend_at + 2..]);
        let crc = field(&yend, "pcrc32=")
            .and_then(|v| u32::from_str_radix(&v, 16).ok())
            .expect("=yend pcrc32");
        let at = k * stride;
        let mut at_end = at;
        for piece in [status.as_bytes(), body, b".\r\n"] {
            pool.bytes()[at_end..at_end + piece.len()].copy_from_slice(piece);
            at_end += piece.len();
        }
        rp.lens.push(at_end - at);
        rp.hdrs.push(status.len() + hdr);
        rp.parts.push(end + 1 - begin);
        rp.crcs.push(crc);
    }
    rp.pool = pool;
    rp
}

// Seeded random parts of REAL_PART bytes, as consecutive parts of one file,
// encoded here with rapidyenc's escaping, in a pool of REAL_SYNTH_POOL bytes.
const REAL_SYNTH_POOL: usize = 256 << 20;
fn real_synth_bodies() -> Vec<(String, Vec<u8>)> {
    let n = REAL_SYNTH_POOL / (encode_max_len(REAL_PART) + 512).div_ceil(4096) / 4096;
    let mut state = 0x5eed_7ea1_0001u64;
    let mut data = vec![0u8; REAL_PART];
    (0..n)
        .map(|k| {
            for b in data.iter_mut() {
                *b = splitmix(&mut state) as u8;
            }
            let p = k + 1;
            let mut body = format!(
                "=ybegin part={p} total={n} line={REAL_LINE} size={} name=synth.bin\r\n=ypart begin={} end={}\r\n",
                n * REAL_PART,
                k * REAL_PART + 1,
                p * REAL_PART
            )
            .into_bytes();
            let enc = encode_article(&data);
            body.extend_from_slice(&enc[..enc.len() - TRAILER.len()]);
            body.extend_from_slice(format!("\r\n=yend size={REAL_PART} part={p} pcrc32={:08x}\r\n", crc32(&data)).as_bytes());
            (format!("synth{p}"), body)
        })
        .collect()
}

// Captured article bodies: SABnzbd's 1000 MB reference test post (48 RAR
// volumes of 37 segments, line=128, 716,800-byte segments). Each file is
// one NNTP BODY payload byte for byte as it arrived on the wire: dot-stuffed
// (none of these lines needed it), CRLF lines, the =ybegin/=ypart/=yend lines
// included, without the terminating dot line, which the loader appends.
// fNNN-sNNNNN is the post's file index (0-based) and segment number. The
// repository keeps 12 (engines/weaver-yenc/fixtures/real-articles):
//   full segments: f000-s00001 f009-s00001 f018-s00001 f027-s00001
//     f038-s00001 f047-s00027 (716,800 decoded bytes each);
//   last segments: f041-s00009 (14,404) f003-s00037 f013-s00037
//     f022-s00037 f032-s00037 (409,600 each) f043-s00003 (681,368).
// WEAVER_YENC_REAL_DIR points at another directory of such files for a wider
// run. Full and last are told apart by the size `=ypart` declares.
fn real_file_bodies(src: RealSrc) -> (String, Vec<(String, Vec<u8>)>) {
    let dir = std::env::var(REAL_DIR_ENV).unwrap_or_else(|_| {
        concat!(env!("CARGO_MANIFEST_DIR"), "/../fixtures/real-articles").to_string()
    });
    let mut names: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{dir}: {e}"))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "body"))
        .collect();
    names.sort();
    let bodies = names
        .into_iter()
        .map(|p| {
            (
                p.file_stem().unwrap().to_string_lossy().into_owned(),
                std::fs::read(&p).unwrap(),
            )
        })
        .filter(|(_, b)| {
            let text = String::from_utf8_lossy(&b[..b.len().min(512)]);
            let full = text.contains(" begin=") && {
                let f = |key: &str| {
                    text.split([' ', '\r', '\n'])
                        .find_map(|w| w.strip_prefix(key))
                        .and_then(|v| v.parse::<usize>().ok())
                };
                f("end=")
                    .zip(f("begin="))
                    .is_some_and(|(e, b)| e + 1 - b == REAL_PART)
            };
            full == (src == RealSrc::Full)
        })
        .collect::<Vec<_>>();
    assert!(
        !bodies.is_empty(),
        "{dir}: no {} bodies",
        if src == RealSrc::Full { "full" } else { "last" }
    );
    (dir, bodies)
}

// One side's connection: its own copy of the pool (the socket), read buffer,
// an output ring with a fresh region per article, and tail scratch.
struct RealSide {
    pool: Paged,
    rbuf: Paged,
    out: Paged,
    slots: usize,
    slot: usize,
    scratch: Paged,
    k: usize,
}

const REAL_OUT_STRIDE: usize = (REAL_PART + 64).div_ceil(4096) * 4096;

// One side's decode call, as `RealSide::article` takes it: (first call of the
// article, input, output) to (written, consumed | end << 62).
type SideDec<'a> = dyn FnMut(bool, &[u8], &mut [u8]) -> (usize, usize) + 'a;

impl RealSide {
    fn new(src: &mut Paged, n: usize) -> Self {
        let mut pool = Paged::new(src.len, 0);
        pool.bytes().copy_from_slice(src.bytes());
        let slots = n.max(REAL_OUT_RING / REAL_OUT_STRIDE);
        let mut out = Paged::new(slots * REAL_OUT_STRIDE, 0);
        // Write every page of the ring once, before any timing: a fresh
        // allocation is mapped on first touch, and that page fault would land
        // in the first pass's samples. Production reuses freed memory. The
        // byte goes through `black_box` so the store into zeroed memory is
        // not dropped as a no-op.
        for page in out.bytes().chunks_mut(4096) {
            page[0] = black_box(0);
        }
        RealSide {
            pool,
            rbuf: Paged::new(READ_BUF + 64, 0),
            out,
            slots,
            slot: 0,
            scratch: Paged::new(TAIL_PROBE + 64, 0),
            k: 0,
        }
    }

    // Decode the next article as the fused decoder does on a download
    // connection (weaver-nntp fused_yenc.rs process_body and
    // next_body_input_len; blocking.rs stream_yenc_article_response): each
    // socket read is copied into the read buffer (untimed), the status and
    // header lines are left to the line parser, then the kernel takes the
    // buffered body, trimmed to the room left in the article's header-sized
    // output, and once that is full a tail probe into scratch finds the
    // trailer. The output goes to the next slot of the ring. Returns
    // (ns in decode calls, article index, output slot). `log` records every
    // call's (consumed, written, end).
    fn article(
        &mut self,
        rp: &RealPool,
        read: usize,
        dec: &mut impl FnMut(bool, &[u8], &mut [u8]) -> (usize, usize),
        mut log: Option<&mut Vec<(usize, usize, i32)>>,
    ) -> (u64, usize, usize) {
        let n = rp.lens.len();
        let k = self.k;
        self.k = if k + 1 == n { 0 } else { k + 1 };
        let slot = self.slot;
        self.slot = if slot + 1 == self.slots { 0 } else { slot + 1 };
        let part = rp.parts[k];
        let pool = self.pool.bytes();
        let t = &pool[k * rp.stride..k * rp.stride + rp.lens[k]];
        let rb = self.rbuf.bytes();
        let o = &mut self.out.bytes()[slot * REAL_OUT_STRIDE..slot * REAL_OUT_STRIDE + part];
        let sc = self.scratch.bytes();
        let (mut pos, mut off, mut dlen, mut skip) = (0usize, 0usize, 0usize, rp.hdrs[k]);
        let (mut written, mut ns, mut first) = (0usize, 0u64, true);
        loop {
            assert!(
                pos < t.len(),
                "article {k}: transcript ended before the body's end"
            );
            // BytesMut::reserve(SOCKET_READ) before the read: the consumed
            // front is reclaimed once the tail has no room.
            if READ_BUF - (off + dlen) < SOCKET_READ {
                assert!(off >= dlen, "article {k}: read buffer would grow");
                rb.copy_within(off..off + dlen, 0);
                off = 0;
            }
            let got = read.min(t.len() - pos);
            rb[off + dlen..off + dlen + got].copy_from_slice(&t[pos..pos + got]);
            pos += got;
            dlen += got;
            let s = skip.min(dlen);
            off += s;
            dlen -= s;
            skip -= s;
            while dlen > 0 {
                let room = part - written;
                let tail = room == 0;
                let il = if tail {
                    dlen.min(TAIL_PROBE)
                } else {
                    dlen.min(room)
                };
                let src = &rb[off..off + il];
                let dst: &mut [u8] = if tail {
                    &mut sc[..il]
                } else {
                    &mut o[written..written + il]
                };
                let t0 = Instant::now();
                let r = dec(first, black_box(src), dst);
                ns += t0.elapsed().as_nanos() as u64;
                first = false;
                let (c, w, e) = unpack(r);
                assert!(e >= 0 && c <= il, "article {k}: decode error {r:?}");
                if let Some(l) = log.as_deref_mut() {
                    l.push((c, w, e));
                }
                if tail {
                    assert!(w == 0, "article {k}: bytes past the declared part");
                } else {
                    written += w;
                }
                off += c;
                dlen -= c;
                if e != 0 {
                    assert!(
                        written == part,
                        "article {k}: {written} of {part} bytes decoded, end {e}"
                    );
                    return (ns, k, slot);
                }
                if c < il {
                    break;
                }
            }
        }
    }

    fn output(&mut self, slot: usize, part: usize) -> &[u8] {
        &self.out.bytes()[slot * REAL_OUT_STRIDE..slot * REAL_OUT_STRIDE + part]
    }
}

fn real_pool(src: RealSrc) -> (String, RealPool) {
    let (what, bodies) = match src {
        RealSrc::Synth => ("seeded".to_string(), real_synth_bodies()),
        _ => real_file_bodies(src),
    };
    let rp = real_pool_of(bodies);
    assert!(
        rp.parts.iter().all(|&p| p <= REAL_PART),
        "a part larger than {REAL_PART} bytes"
    );
    (what, rp)
}

#[allow(clippy::too_many_arguments)]
fn run_real(
    opts: &Options,
    fname: &str,
    src: RealSrc,
    read: usize,
    n_samples: usize,
    target_ns: u128,
    arch: &str,
    cpu: &str,
) {
    let (what, mut rp) = real_pool(src);
    let n = rp.lens.len();
    if opts.hash_fixtures {
        let mut h = 0xcbf2_9ce4_8422_2325u64;
        for k in 0..n {
            fnv1a(
                &mut h,
                &rp.pool.bytes()[k * rp.stride..k * rp.stride + rp.lens[k]],
            );
        }
        println!(
            "HASH\t{fname}\t{n}\t{}\t{h:016x}",
            rp.lens.iter().sum::<usize>()
        );
        return;
    }
    let mut ws = RealSide::new(&mut rp.pool, n);
    let mut rs = RealSide::new(&mut rp.pool, n);
    // The scalar tier, as a second reference under --verify.
    let mut ss = if opts.verify {
        Some(RealSide::new(&mut rp.pool, n))
    } else {
        None
    };
    let total: usize = rp.parts.iter().sum();
    // What the kernel is handed of each article: the body after the header
    // lines, with the dot line.
    let body = |k: usize| rp.lens[k] - rp.hdrs[k];
    // The bytes column, as the other fixtures' input size: the mean body of
    // the pool's articles. Times are ns per body of that size (ns per body
    // byte over the sample, times the mean), so both sides compare over the
    // same mix of sizes.
    let bytes = (0..n).map(body).sum::<usize>() / n;
    if opts.verify {
        println!("REAL\t{fname}\t{n} articles\t{total} decoded bytes\t{what}");
    }
    let chunk_dec = |cfn: ChunkFn| {
        let mut st = ChunkState::body(Some(REAL_LINE));
        move |reset: bool, s: &[u8], d: &mut [u8]| {
            if reset {
                st = ChunkState::body(Some(REAL_LINE));
            }
            // SAFETY: `resolve_chunk` checked the tier runs; `d` holds
            // `s.len()` bytes.
            unsafe { cfn(s, d, &mut st) }
        }
    };
    let mut dscalar = chunk_dec(resolve_chunk(Tier::Scalar).unwrap());
    for tier in TIERS {
        let Some(cfn) = resolve_chunk(tier) else {
            continue;
        };
        if opts.tier.as_deref().is_some_and(|t| t != tier.as_str()) {
            continue;
        }
        let (kid, kname) = kernel_for(tier);
        // SAFETY: force checks CPU support itself; single-threaded.
        if unsafe { weaver_rapidyenc_decode_force(kid) } != 1 {
            println!(
                "SKIP\t{fname}\t{}\trapidyenc {kname} refused",
                tier.as_str()
            );
            continue;
        }
        let mut dw = chunk_dec(cfn);
        let mut rst = 0i32; // YDEC_STATE_CRLF
        let mut dr = |reset: bool, s: &[u8], d: &mut [u8]| {
            if reset {
                rst = 0;
            }
            let (mut c, mut w) = (0u64, 0u64);
            // SAFETY: `d` holds `s.len()` bytes; distinct buffers.
            let e = unsafe {
                weaver_rapidyenc_decode_end_state(
                    s.as_ptr().cast(),
                    d.as_mut_ptr().cast(),
                    s.len() as u64,
                    &mut c,
                    &mut w,
                    &mut rst,
                )
            };
            (w as usize, c as usize | (e as usize) << 62)
        };
        ws.k = 0;
        rs.k = 0;
        // Equality first: the same calls with the same answers, and the
        // decoded part with the CRC its `=yend` declares, on a few
        // articles; `--verify` on all, also against the scalar tier.
        let checks = if opts.verify { n } else { n.min(3) };
        for _ in 0..checks {
            let (mut lw, mut lr) = (Vec::new(), Vec::new());
            let (_, k, sw) = ws.article(&rp, read, &mut dw, Some(&mut lw));
            let (_, _, sr) = rs.article(&rp, read, &mut dr, Some(&mut lr));
            let part = rp.parts[k];
            let wo = ws.output(sw, part).to_vec();
            let same_scalar = ss.as_mut().is_none_or(|s| {
                s.k = k;
                let (_, _, so) = s.article(&rp, read, &mut dscalar, None);
                s.output(so, part) == wo
            });
            assert!(
                lw == lr && wo == rs.output(sr, part) && crc32(&wo) == rp.crcs[k] && same_scalar,
                "{fname} article {k}: weaver {} {lw:?} differs from rapidyenc {kname} {lr:?}, the scalar tier, or the =yend pcrc32",
                tier.as_str()
            );
        }
        if opts.verify {
            println!(
                "VERIFY\tok\t{fname}\tend\t{}\t{kname}\t{n} articles",
                tier.as_str()
            );
            continue;
        }
        if let Some(side) = opts.perf.as_deref() {
            let iters = (4usize << 30) / bytes;
            for _ in 0..iters {
                if side == "weaver" {
                    black_box(ws.article(&rp, read, &mut dw, None));
                } else if side == "rapidyenc" {
                    black_box(rs.article(&rp, read, &mut dr, None));
                }
            }
            println!(
                "PERF\t{side}\t{fname}\tend\t{}\t{iters}\t{bytes}",
                tier.as_str()
            );
            continue;
        }
        // Articles per sample: enough decode time for one batch, and at
        // least one pass over the pool.
        let per_sample = |side: &mut RealSide, dec: &mut SideDec<'_>| {
            let mut d = |a: bool, s: &[u8], o: &mut [u8]| dec(a, s, o);
            let mut ns = 0u64;
            for _ in 0..n {
                ns += side.article(&rp, read, &mut d, None).0;
            }
            let per = ns as u128 / n as u128;
            ((target_ns / per.max(1)) as usize + 1).max(n)
        };
        let r_arts = per_sample(&mut rs, &mut dr);
        let w_arts = per_sample(&mut ws, &mut dw);
        let sample_real = |side: &mut RealSide, arts: usize, dec: &mut SideDec<'_>| {
            let mut d = |a: bool, s: &[u8], o: &mut [u8]| dec(a, s, o);
            let (mut ns, mut got) = (0u64, 0usize);
            for _ in 0..arts {
                let (t, k, _) = side.article(&rp, read, &mut d, None);
                ns += t;
                got += body(k);
            }
            ns as f64 / got as f64 * bytes as f64
        };
        let (mut w_ns, mut r_ns) = (Vec::new(), Vec::new());
        // Each pair of samples swaps which side runs first.
        for s in 0..n_samples {
            if s % 2 == 0 {
                w_ns.push(sample_real(&mut ws, w_arts, &mut dw));
                r_ns.push(sample_real(&mut rs, r_arts, &mut dr));
            } else {
                r_ns.push(sample_real(&mut rs, r_arts, &mut dr));
                w_ns.push(sample_real(&mut ws, w_arts, &mut dw));
            }
        }
        let (wmin, wmed) = min_med(w_ns);
        let (rmin, rmed) = min_med(r_ns);
        println!(
            "{fname:<17} end {bytes:>7} | {:<12} {wmin:>10.0} {wmed:>10.0} | {kname:<7} {rmin:>10.0} {rmed:>10.0} | {:>6.3} {:>6.3}",
            tier.as_str(),
            rmin / wmin,
            rmed / wmed
        );
        println!(
            "TSV\t{arch}\t{cpu}\t{fname}\tend\t{bytes}\t{}\t{wmin:.1}\t{wmed:.1}\t{kname}\t{rmin:.1}\t{rmed:.1}\t{:.4}\t{:.4}",
            tier.as_str(),
            rmin / wmin,
            rmed / wmed
        );
    }
}

// ---- the two decoders -----------------------------------------------------

// Both sides answer `(written, consumed | end << 62)` (bench::unpack), in
// two registers.
#[inline(always)]
fn weaver_once(wfn: DecodeFn, end: bool, src: &[u8], dst: &mut [u8]) -> (usize, usize) {
    // SAFETY: `resolve` checked the tier runs; `dst` holds at least
    // `src.len()` bytes.
    unsafe { wfn(src, dst, end) }
}

#[inline(always)]
fn rapidyenc_once(end: bool, src: &[u8], dst: &mut [u8]) -> (usize, usize) {
    // SAFETY: `dst` holds at least `src.len()` bytes; distinct buffers.
    unsafe {
        if end {
            let (mut c, mut w) = (0u64, 0u64);
            let e = weaver_rapidyenc_decode_end(
                src.as_ptr().cast(),
                dst.as_mut_ptr().cast(),
                src.len() as u64,
                &mut c,
                &mut w,
            );
            (w as usize, c as usize | (e as usize) << 62)
        } else {
            let w = weaver_rapidyenc_decode_crlf(
                src.as_ptr().cast(),
                dst.as_mut_ptr().cast(),
                src.len() as u64,
            );
            (w as usize, src.len())
        }
    }
}

// ---- timing (as encode-bench) ---------------------------------------------

fn calibrate(target_ns: u128, mut f: impl FnMut()) -> u64 {
    f();
    let mut iters = 1u64;
    loop {
        let t = Instant::now();
        for _ in 0..iters {
            f();
        }
        if t.elapsed().as_nanos() >= target_ns || iters >= 1 << 30 {
            return iters;
        }
        iters *= 2;
    }
}

fn sample(iters: u64, f: &mut impl FnMut()) -> f64 {
    let t = Instant::now();
    for _ in 0..iters {
        f();
    }
    t.elapsed().as_nanos() as f64 / iters as f64
}

fn min_med(mut ns: Vec<f64>) -> (f64, f64) {
    ns.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (ns[0], ns[ns.len() / 2])
}

fn cpu_model() -> String {
    #[cfg(target_arch = "x86_64")]
    {
        let mut bytes = Vec::new();
        for leaf in 0x8000_0002u32..=0x8000_0004 {
            #[allow(unused_unsafe)]
            // SAFETY: CPUID exists on every x86_64 CPU.
            let r = unsafe { std::arch::x86_64::__cpuid(leaf) };
            for word in [r.eax, r.ebx, r.ecx, r.edx] {
                bytes.extend_from_slice(&word.to_le_bytes());
            }
        }
        let s = String::from_utf8_lossy(&bytes)
            .trim_matches(char::from(0))
            .trim()
            .to_string();
        if !s.is_empty() {
            return s;
        }
    }
    if cfg!(target_os = "macos")
        && let Ok(out) = std::process::Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
    {
        let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !s.is_empty() {
            return s;
        }
    }
    "unknown".to_string()
}

fn aslr_state() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        unsafe extern "C" {
            fn personality(persona: core::ffi::c_ulong) -> core::ffi::c_int;
        }
        // SAFETY: 0xffffffff only queries the current persona.
        let p = unsafe { personality(0xffff_ffff) };
        if p >= 0 && p & 0x0040000 != 0 {
            return "off (ADDR_NO_RANDOMIZE, e.g. setarch -R)";
        }
        return "ON: load addresses change per run; run under setarch -R";
    }
    #[allow(unreachable_code)]
    "not checked on this OS"
}

fn main() {
    let opts = parse_args();
    // SAFETY: plain init call.
    unsafe { weaver_rapidyenc_decode_init() };
    let default_kernel = unsafe { weaver_rapidyenc_decode_kernel() };
    let cpu = cpu_model();
    let arch = std::env::consts::ARCH;
    let (n_samples, target_ns) = if opts.quick {
        (5usize, 1_000_000u128)
    } else {
        (21, 5_000_000)
    };
    println!("weaver-yenc decode bench (line size {LINE_SIZE}, CRLF, trailer \\r\\n=yend\\r\\n)");
    println!("host:      {arch} {} / {cpu}", std::env::consts::OS);
    let tiers: Vec<&str> = TIERS
        .iter()
        .filter(|t| runs(**t))
        .map(|t| t.as_str())
        .collect();
    println!(
        "weaver:    tiers {}; dispatch picks {}",
        tiers.join(", "),
        weaver_yenc::simd::selected_decoder_tier().as_str()
    );
    println!("rapidyenc: default kernel {default_kernel:#x}");
    println!("aslr:      {}", aslr_state());
    println!(
        "samples:   {n_samples} per side, interleaved; each a batch of >= {} ms\n",
        target_ns / 1_000_000
    );
    println!(
        "TSVHEAD\tarch\tcpu\tfixture\tmode\tbytes\tweaver_tier\tweaver_min_ns\tweaver_median_ns\trapidyenc_kernel\trapidyenc_min_ns\trapidyenc_median_ns\tratio_min\tratio_median"
    );

    for (fname, make) in FIXTURES {
        if opts.fixture.as_deref().is_some_and(|f| f != fname) {
            continue;
        }
        // One article; both sides read the same buffer. The loops below index
        // it as article `k` of `n`, which is always 0 of 1.
        let enc = encode_article(&make());
        let mut fixed = Paged::new(enc.len(), 0);
        fixed.bytes().copy_from_slice(&enc);
        let stride = enc.len();
        let lens = [stride];
        let pool: &[u8] = fixed.bytes();
        let pool_r = pool;
        let n = lens.len();
        let art = |k: usize| &pool[k * stride..k * stride + lens[k]];
        let art_r = |k: usize| &pool_r[k * stride..k * stride + lens[k]];
        if opts.hash_fixtures {
            let mut h = 0xcbf2_9ce4_8422_2325u64;
            for k in 0..n {
                fnv1a(&mut h, art(k));
            }
            println!(
                "HASH\t{fname}\t{n}\t{}\t{h:016x}",
                lens.iter().sum::<usize>()
            );
            continue;
        }
        let cap = *lens.iter().max().unwrap() + 64;
        let mut wdst_buf = Paged::new(cap, DST_PAGE_OFFSET);
        let mut rdst_buf = Paged::new(cap, DST_PAGE_OFFSET);
        let wdst = wdst_buf.bytes();
        let rdst = rdst_buf.bytes();
        let bytes = lens[0];

        for tier in TIERS {
            // The tier's kernel, resolved once: the timed call does no
            // feature check.
            let Some(wfn) = resolve(tier) else {
                continue;
            };
            if opts.tier.as_deref().is_some_and(|t| t != tier.as_str()) {
                continue;
            }
            let (kid, kname) = kernel_for(tier);
            // SAFETY: force checks CPU support itself; single-threaded.
            if unsafe { weaver_rapidyenc_decode_force(kid) } != 1 {
                println!(
                    "SKIP\t{fname}\t{}\trapidyenc {kname} refused",
                    tier.as_str()
                );
                continue;
            }
            for (mode, end) in [("raw", false), ("end", true)] {
                if opts.mode.as_deref().is_some_and(|m| m != mode) {
                    continue;
                }
                // Equality first; `--verify` also checks every prefix cut of
                // the article, then times nothing.
                let checks: Vec<usize> = if opts.verify {
                    (0..n).collect()
                } else {
                    vec![0, n / 2, n - 1]
                };
                for k in checks {
                    let a = art(k);
                    let r = unpack(rapidyenc_once(end, a, rdst));
                    let w = unpack(weaver_once(wfn, end, a, wdst));
                    assert!(
                        r == w && wdst[..w.1] == rdst[..r.1],
                        "{fname} {mode} article {k}: weaver {} {w:?} differs from rapidyenc {kname} {r:?}",
                        tier.as_str()
                    );
                }
                if opts.verify {
                    // Prefix cuts land the end inside escapes, CRLFs, dots
                    // and the trailer; both sides must agree on all of them.
                    let a = art(0);
                    let cuts = (0..a.len().min(4096)).chain(a.len().saturating_sub(4096)..a.len());
                    for cut in cuts {
                        let p = &a[..cut];
                        let r = unpack(rapidyenc_once(end, p, rdst));
                        let w = unpack(weaver_once(wfn, end, p, wdst));
                        assert!(
                            r == w && wdst[..w.1] == rdst[..r.1],
                            "{fname} {mode} cut {cut}: weaver {} {w:?} differs from rapidyenc {kname} {r:?}",
                            tier.as_str()
                        );
                    }
                    println!(
                        "VERIFY\tok\t{fname}\t{mode}\t{}\t{kname}\t{n} articles",
                        tier.as_str()
                    );
                    continue;
                }
                if let Some(side) = opts.perf.as_deref() {
                    let iters = (4usize << 30) / bytes;
                    for i in 0..iters {
                        let a = art(i % n);
                        if side == "weaver" {
                            black_box(weaver_once(wfn, end, black_box(a), wdst));
                        } else if side == "rapidyenc" {
                            black_box(rapidyenc_once(end, black_box(art_r(i % n)), rdst));
                        }
                    }
                    println!(
                        "PERF\t{side}\t{fname}\t{mode}\t{}\t{iters}\t{bytes}",
                        tier.as_str()
                    );
                    continue;
                }
                let (mut kw, mut kr) = (0usize, 0usize);
                let mut run_w = || {
                    black_box(weaver_once(wfn, end, black_box(art(kw)), wdst));
                    kw = if kw + 1 == n { 0 } else { kw + 1 };
                };
                let mut run_r = || {
                    black_box(rapidyenc_once(end, black_box(art_r(kr)), rdst));
                    kr = if kr + 1 == n { 0 } else { kr + 1 };
                };
                let r_iters = calibrate(target_ns, &mut run_r);
                let w_iters = calibrate(target_ns, &mut run_w);
                let (mut w_ns, mut r_ns) = (Vec::new(), Vec::new());
                // Each pair of samples swaps which side runs first.
                for s in 0..n_samples {
                    if s % 2 == 0 {
                        w_ns.push(sample(w_iters, &mut run_w));
                        r_ns.push(sample(r_iters, &mut run_r));
                    } else {
                        r_ns.push(sample(r_iters, &mut run_r));
                        w_ns.push(sample(w_iters, &mut run_w));
                    }
                }
                let (wmin, wmed) = min_med(w_ns);
                let (rmin, rmed) = min_med(r_ns);
                println!(
                    "{fname:<17} {mode:<3} {bytes:>7} | {:<12} {wmin:>10.0} {wmed:>10.0} | {kname:<7} {rmin:>10.0} {rmed:>10.0} | {:>6.3} {:>6.3}",
                    tier.as_str(),
                    rmin / wmin,
                    rmed / wmed
                );
                println!(
                    "TSV\t{arch}\t{cpu}\t{fname}\t{mode}\t{bytes}\t{}\t{wmin:.1}\t{wmed:.1}\t{kname}\t{rmin:.1}\t{rmed:.1}\t{:.4}\t{:.4}",
                    tier.as_str(),
                    rmin / wmin,
                    rmed / wmed
                );
            }
        }
    }
    for (fname, src, read) in REAL {
        if opts.fixture.as_deref().is_some_and(|f| f != fname)
            || opts.mode.as_deref().is_some_and(|m| m != "end")
        {
            continue;
        }
        run_real(&opts, fname, src, read, n_samples, target_ns, arch, &cpu);
    }
    unsafe { weaver_rapidyenc_decode_force(default_kernel) };
}
