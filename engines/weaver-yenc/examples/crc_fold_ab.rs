//! Interleaved A/B harness for the yEnc decode CRC32 path.
//!
//! Measurement only: nothing here changes a production path. It compares, in
//! one process and on the same bytes, the CRC32 weaver ships (the `Crc32`
//! wrapper, which takes the 256-bit carry-less fold where the CPU selects it),
//! `crc-fast` called directly (what the wrapper falls back to), and
//! rapidyenc's own CRC as the oracle, alone and fused behind a decode of the
//! 128-column article fixture the parity bench uses.
//!
//!   WEAVER_RAPIDYENC_LIB=/path/to/librapidyenc.so \
//!     cargo run --locked --release -p weaver-yenc --example crc_fold_ab
//!
//! Without `WEAVER_RAPIDYENC_LIB` the rapidyenc lanes are skipped.
//!
//! Single-thread mode (no arguments) runs every lane once per round, rotating
//! the lane order each round, and reports the median and minimum time per
//! iteration. Before timing it asserts decoded-byte parity and CRC parity
//! between every lane.
//!
//! Multi-thread mode, `mt <lane> <threads> <iterations-per-thread>`, runs one
//! decode+CRC lane on that many threads at once and prints the wall time and
//! aggregate throughput. Time it from the shell to get the process CPU time.
//!
//! Optional env: `CRC_AB_ROUNDS` (default 7), `CRC_AB_SAMPLE_MS` (default 150).

use std::ffi::c_void;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use weaver_yenc::crc::{Crc32, wide_fold_selected};
use weaver_yenc::decode::decode_rapidyenc;

type DecodeFn = unsafe extern "C" fn(*const c_void, *mut c_void, usize) -> usize;
type CrcFn = unsafe extern "C" fn(*const c_void, usize, u32) -> u32;
type InitFn = unsafe extern "C" fn();
type KernelFn = unsafe extern "C" fn() -> i32;

struct Rapidyenc {
    _lib: libloading::Library,
    decode: DecodeFn,
    crc: CrcFn,
    crc_kernel: i32,
}

impl Rapidyenc {
    fn load() -> Option<Self> {
        let path = std::env::var_os("WEAVER_RAPIDYENC_LIB")?;
        let lib = unsafe { libloading::Library::new(&path) }
            .unwrap_or_else(|err| panic!("WEAVER_RAPIDYENC_LIB={path:?} failed to load: {err}"));
        unsafe {
            (*lib.get::<InitFn>(b"rapidyenc_decode_init").ok()?)();
            (*lib.get::<InitFn>(b"rapidyenc_crc_init").ok()?)();
            let decode = *lib.get::<DecodeFn>(b"rapidyenc_decode").ok()?;
            let crc = *lib.get::<CrcFn>(b"rapidyenc_crc").ok()?;
            let crc_kernel = lib
                .get::<KernelFn>(b"rapidyenc_crc_kernel")
                .map(|f| f())
                .unwrap_or(-1);
            Some(Self {
                _lib: lib,
                decode,
                crc,
                crc_kernel,
            })
        }
    }

    fn decode(&self, input: &[u8], output: &mut [u8]) -> usize {
        assert!(output.len() >= input.len());
        unsafe {
            (self.decode)(
                input.as_ptr() as *const c_void,
                output.as_mut_ptr() as *mut c_void,
                input.len(),
            )
        }
    }

    fn crc(&self, data: &[u8], init: u32) -> u32 {
        unsafe { (self.crc)(data.as_ptr() as *const c_void, data.len(), init) }
    }
}

/// The parity bench's 128-column article body (768 000 decoded bytes).
fn real_yenc_128col_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(800 * 1024);
    let mut col = 0usize;
    for idx in 0..768_000usize {
        let byte = ((idx * 31 + 17) & 0xff) as u8;
        let encoded = byte.wrapping_add(42);
        match encoded {
            0x00 | 0x0a | 0x0d | 0x3d => {
                body.push(b'=');
                body.push(encoded.wrapping_add(64));
                col += 2;
            }
            0x2e if col == 0 => {
                body.push(b'=');
                body.push(encoded.wrapping_add(64));
                col += 2;
            }
            _ => {
                body.push(encoded);
                col += 1;
            }
        }
        if col >= 128 {
            body.extend_from_slice(b"\r\n");
            col = 0;
        }
    }
    if col > 0 {
        body.extend_from_slice(b"\r\n");
    }
    body
}

fn pseudo_random(len: usize, mut seed: u32) -> Vec<u8> {
    (0..len)
        .map(|_| {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (seed >> 24) as u8
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lane {
    CrcShipped,
    CrcCrcFast,
    CrcRapidyenc,
    DecodeWeaver,
    DecodeRapidyenc,
    DecodeCrcShipped,
    DecodeCrcCrcFast,
    DecodeCrcRapidyenc,
}

impl Lane {
    const ALL: [Lane; 8] = [
        Lane::CrcShipped,
        Lane::CrcCrcFast,
        Lane::CrcRapidyenc,
        Lane::DecodeWeaver,
        Lane::DecodeRapidyenc,
        Lane::DecodeCrcShipped,
        Lane::DecodeCrcCrcFast,
        Lane::DecodeCrcRapidyenc,
    ];

    fn name(self) -> &'static str {
        match self {
            Lane::CrcShipped => "crc/weaver-shipped",
            Lane::CrcCrcFast => "crc/crc-fast",
            Lane::CrcRapidyenc => "crc/rapidyenc",
            Lane::DecodeWeaver => "decode/weaver",
            Lane::DecodeRapidyenc => "decode/rapidyenc",
            Lane::DecodeCrcShipped => "decode+crc/weaver-shipped",
            Lane::DecodeCrcCrcFast => "decode+crc/weaver+crc-fast",
            Lane::DecodeCrcRapidyenc => "decode+crc/rapidyenc",
        }
    }

    fn parse(name: &str) -> Option<Lane> {
        Lane::ALL.into_iter().find(|lane| lane.name() == name)
    }

    fn needs_rapidyenc(self) -> bool {
        matches!(
            self,
            Lane::CrcRapidyenc | Lane::DecodeRapidyenc | Lane::DecodeCrcRapidyenc
        )
    }
}

/// Per-thread working set: the encoded article, the decoded bytes and an
/// output buffer, so threads never share a cache line they write.
struct Work {
    encoded: Vec<u8>,
    decoded: Vec<u8>,
    out: Vec<u8>,
}

impl Work {
    fn new(encoded: &[u8], decoded: &[u8]) -> Self {
        Self {
            encoded: encoded.to_vec(),
            decoded: decoded.to_vec(),
            out: vec![0u8; encoded.len() + 64],
        }
    }

    #[inline]
    fn run(&mut self, lane: Lane, rapid: Option<&Rapidyenc>) -> u32 {
        match lane {
            Lane::CrcShipped => {
                let mut crc = Crc32::new();
                crc.update(black_box(&self.decoded));
                crc.finalize()
            }
            Lane::CrcCrcFast => crc_fast::checksum(
                crc_fast::CrcAlgorithm::Crc32IsoHdlc,
                black_box(&self.decoded),
            ) as u32,
            Lane::CrcRapidyenc => rapid.unwrap().crc(black_box(&self.decoded), 0),
            Lane::DecodeWeaver => {
                decode_rapidyenc(black_box(&self.encoded), &mut self.out).unwrap() as u32
            }
            Lane::DecodeRapidyenc => rapid
                .unwrap()
                .decode(black_box(&self.encoded), &mut self.out)
                as u32,
            Lane::DecodeCrcShipped => {
                let n = decode_rapidyenc(black_box(&self.encoded), &mut self.out).unwrap();
                let mut crc = Crc32::new();
                crc.update(&self.out[..n]);
                crc.finalize()
            }
            Lane::DecodeCrcCrcFast => {
                let n = decode_rapidyenc(black_box(&self.encoded), &mut self.out).unwrap();
                crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, &self.out[..n]) as u32
            }
            Lane::DecodeCrcRapidyenc => {
                let rapid = rapid.unwrap();
                let n = rapid.decode(black_box(&self.encoded), &mut self.out);
                rapid.crc(&self.out[..n], 0)
            }
        }
    }
}

fn reference_crc(data: &[u8], init: u32) -> u32 {
    // Bitwise CRC-32/ISO-HDLC, no table: the independent oracle for parity.
    let mut crc = !init;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Byte and CRC parity between every lane, plus an adversarial sweep of the
/// CRC implementations against the bitwise reference.
fn parity(encoded: &[u8], decoded: &[u8], rapid: Option<&Rapidyenc>) -> u32 {
    let expected = reference_crc(decoded, 0);
    let mut out = vec![0u8; encoded.len() + 64];
    let n = decode_rapidyenc(encoded, &mut out).unwrap();
    assert_eq!(&out[..n], decoded, "weaver decoded bytes");
    if let Some(rapid) = rapid {
        let mut rout = vec![0u8; encoded.len() + 64];
        let rn = rapid.decode(encoded, &mut rout);
        assert_eq!(&rout[..rn], decoded, "rapidyenc decoded bytes");
    }
    let mut work = Work::new(encoded, decoded);
    for lane in Lane::ALL {
        if lane.needs_rapidyenc() && rapid.is_none() {
            continue;
        }
        if matches!(lane, Lane::DecodeWeaver | Lane::DecodeRapidyenc) {
            assert_eq!(
                work.run(lane, rapid) as usize,
                decoded.len(),
                "{}",
                lane.name()
            );
        } else {
            assert_eq!(work.run(lane, rapid), expected, "{}", lane.name());
        }
    }

    let random = pseudo_random(4096 + 64, 0x2545_f491);
    let patterns: [(&str, Vec<u8>); 4] = [
        ("random", random),
        ("zeros", vec![0u8; 4096 + 64]),
        ("ones", vec![0xffu8; 4096 + 64]),
        (
            "alternating",
            (0..4096 + 64)
                .map(|i| if i % 2 == 0 { 0xaa } else { 0x55 })
                .collect(),
        ),
    ];
    let mut checked = 0usize;
    for (name, data) in &patterns {
        for offset in [0usize, 1, 7, 31] {
            for len in (0..=1100usize).chain([2048, 4095, 4096]) {
                let input = &data[offset..offset + len];
                let want = reference_crc(input, 0);
                let mut crc = Crc32::new();
                crc.update(input);
                assert_eq!(
                    crc.finalize(),
                    want,
                    "{name} shipped off {offset} len {len}"
                );
                let fast = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, input) as u32;
                assert_eq!(fast, want, "{name} crc-fast off {offset} len {len}");
                if let Some(rapid) = rapid {
                    assert_eq!(rapid.crc(input, 0), want, "{name} rapidyenc len {len}");
                    let init = 0x9e37_79b9;
                    assert_eq!(
                        rapid.crc(input, init),
                        reference_crc(input, init),
                        "{name} rapidyenc init len {len}"
                    );
                }
                checked += 1;
            }
        }
    }
    eprintln!("parity ok: decoded bytes + CRC {expected:#010x}; {checked} adversarial inputs");
    expected
}

fn median(sorted: &[f64]) -> f64 {
    sorted[sorted.len() / 2]
}

fn single_thread(encoded: &[u8], decoded: &[u8], rapid: Option<&Rapidyenc>) {
    let rounds: usize = std::env::var("CRC_AB_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7);
    let sample = Duration::from_millis(
        std::env::var("CRC_AB_SAMPLE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(150),
    );
    let lanes: Vec<Lane> = Lane::ALL
        .into_iter()
        .filter(|lane| rapid.is_some() || !lane.needs_rapidyenc())
        .collect();
    let mut work = Work::new(encoded, decoded);

    // Calibrate each lane to roughly `sample` per timed run.
    let iters: Vec<u64> = lanes
        .iter()
        .map(|&lane| {
            let start = Instant::now();
            let mut n = 0u64;
            while start.elapsed() < sample / 4 {
                black_box(work.run(lane, rapid));
                n += 1;
            }
            (n * 4).max(1)
        })
        .collect();

    let mut ns: Vec<Vec<f64>> = vec![Vec::with_capacity(rounds); lanes.len()];
    for round in 0..rounds {
        for k in 0..lanes.len() {
            let idx = (k + round) % lanes.len();
            let lane = lanes[idx];
            let start = Instant::now();
            for _ in 0..iters[idx] {
                black_box(work.run(lane, rapid));
            }
            ns[idx].push(start.elapsed().as_nanos() as f64 / iters[idx] as f64);
        }
    }

    println!(
        "{:<28} {:>11} {:>11} {:>9} {:>9}",
        "lane", "median us", "min us", "GB/s", "spread%"
    );
    for (idx, lane) in lanes.iter().enumerate() {
        let mut s = ns[idx].clone();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = median(&s);
        let spread = (s[s.len() - 1] - s[0]) / med * 100.0;
        println!(
            "{:<28} {:>11.2} {:>11.2} {:>9.2} {:>9.1}",
            lane.name(),
            med / 1e3,
            s[0] / 1e3,
            decoded.len() as f64 / med,
            spread
        );
    }
    println!(
        "(GB/s over {} decoded bytes; median of {rounds} interleaved rounds)",
        decoded.len()
    );
}

fn multi_thread(
    lane: Lane,
    threads: usize,
    iters: u64,
    encoded: &[u8],
    decoded: &[u8],
    rapid: Option<Arc<Rapidyenc>>,
) {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let rapid = rapid.clone();
            let mut work = Work::new(encoded, decoded);
            std::thread::spawn(move || {
                barrier.wait();
                let mut acc = 0u32;
                for _ in 0..iters {
                    acc ^= work.run(lane, rapid.as_deref());
                }
                black_box(acc);
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    for handle in handles {
        handle.join().unwrap();
    }
    let wall = start.elapsed().as_secs_f64();
    let bytes = decoded.len() as f64 * iters as f64 * threads as f64;
    println!(
        "mt lane={} threads={threads} iters={iters} wall_s={wall:.4} GB/s={:.2}",
        lane.name(),
        bytes / wall / 1e9
    );
}

fn main() {
    let rapid = Rapidyenc::load();
    let encoded = real_yenc_128col_body();
    let decoded: Vec<u8> = (0..768_000usize)
        .map(|idx| ((idx * 31 + 17) & 0xff) as u8)
        .collect();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("mt") {
        let lane = Lane::parse(&args[1]).unwrap_or_else(|| panic!("unknown lane {}", args[1]));
        let threads: usize = args[2].parse().unwrap();
        let iters: u64 = args[3].parse().unwrap();
        assert!(
            !lane.needs_rapidyenc() || rapid.is_some(),
            "lane needs WEAVER_RAPIDYENC_LIB"
        );
        multi_thread(
            lane,
            threads,
            iters,
            &encoded,
            &decoded,
            rapid.map(Arc::new),
        );
        return;
    }

    println!(
        "arch={} wide_fold_selected={} crc-fast target={} rapidyenc crc kernel={}",
        std::env::consts::ARCH,
        wide_fold_selected(),
        crc_fast::get_calculator_target(crc_fast::CrcAlgorithm::Crc32IsoHdlc),
        rapid
            .as_ref()
            .map_or("absent".to_string(), |r| format!("{:#x}", r.crc_kernel))
    );
    println!(
        "fixture: {} encoded -> {} decoded bytes",
        encoded.len(),
        decoded.len()
    );
    parity(&encoded, &decoded, rapid.as_ref());
    single_thread(&encoded, &decoded, rapid.as_ref());
}
