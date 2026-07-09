//! Side-by-side parity bench against rapidyenc (RQ2's parity harness).
//!
//! Gated on `WEAVER_RAPIDYENC_LIB` pointing at a rapidyenc shared library
//! (e.g. `librapidyenc.dylib` / `librapidyenc.so` from a cmake build of
//! <https://github.com/animetosho/rapidyenc>). Without it the bench registers
//! nothing and prints a skip note, so `cargo bench` stays green everywhere.
//!
//! Before timing anything it asserts byte-for-byte decode parity and CRC
//! parity between weaver-yenc and rapidyenc on the shared fixture.

use std::env;
use std::ffi::c_void;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use weaver_yenc::crc::Crc32;
use weaver_yenc::decode::decode_rapidyenc;

type DecodeFn = unsafe extern "C" fn(*const c_void, *mut c_void, usize) -> usize;
type CrcFn = unsafe extern "C" fn(*const c_void, usize, u32) -> u32;
type InitFn = unsafe extern "C" fn();
type KernelFn = unsafe extern "C" fn() -> i32;

struct Rapidyenc {
    // Keeps the dlopen handle alive for the raw fn pointers below.
    _lib: libloading::Library,
    decode: DecodeFn,
    crc: CrcFn,
    decode_kernel: i32,
    crc_kernel: i32,
}

impl Rapidyenc {
    fn load() -> Option<Self> {
        let path = env::var_os("WEAVER_RAPIDYENC_LIB")?;
        let lib = match unsafe { libloading::Library::new(&path) } {
            Ok(lib) => lib,
            Err(err) => {
                eprintln!("WEAVER_RAPIDYENC_LIB={path:?} failed to load: {err}");
                return None;
            }
        };

        unsafe {
            let decode_init: libloading::Symbol<'_, InitFn> =
                lib.get(b"rapidyenc_decode_init").ok()?;
            let crc_init: libloading::Symbol<'_, InitFn> = lib.get(b"rapidyenc_crc_init").ok()?;
            decode_init();
            crc_init();

            let decode = *lib.get::<DecodeFn>(b"rapidyenc_decode").ok()?;
            let crc = *lib.get::<CrcFn>(b"rapidyenc_crc").ok()?;
            let decode_kernel = lib
                .get::<KernelFn>(b"rapidyenc_decode_kernel")
                .map(|f| f())
                .unwrap_or(-1);
            let crc_kernel = lib
                .get::<KernelFn>(b"rapidyenc_crc_kernel")
                .map(|f| f())
                .unwrap_or(-1);

            Some(Self {
                _lib: lib,
                decode,
                crc,
                decode_kernel,
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

    fn crc(&self, data: &[u8]) -> u32 {
        unsafe { (self.crc)(data.as_ptr() as *const c_void, data.len(), 0) }
    }
}

/// Column-accurate yEnc encoding at 128 encoded columns (same shape as
/// rapidyenc's own bench article and real Usenet posts).
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

const DECODED_TARGET: usize = 768_000;

/// M-phase fixture: no specials at all. One giant line of `'@'` (0x40, which is
/// not `= \r \n` and decodes to 0x16); no CRLF, no escapes, no dots. Isolates
/// pure fast-path + driver overhead — weaver and rapidyenc should be at parity
/// here; if not, the cost is the loop structure, not the mask/branch handling.
fn clean_body() -> Vec<u8> {
    vec![0x40u8; DECODED_TARGET]
}

/// M-phase fixture: 128-column lines of `'@'` terminated by CRLF, no escapes
/// and no dots. Isolates the CRLF (`specials != eq`) heavy branch — the prime
/// suspect, since it fires on roughly every other 64-byte chunk.
fn crlf_only_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(DECODED_TARGET + DECODED_TARGET / 64 + 64);
    let mut produced = 0usize;
    while produced < DECODED_TARGET {
        let line = 128.min(DECODED_TARGET - produced);
        body.extend(std::iter::repeat_n(0x40u8, line));
        body.extend_from_slice(b"\r\n");
        produced += line;
    }
    body
}

/// M-phase fixture: `'@'` body with ~0.4% `=@` escapes (each decodes to one
/// byte), no CRLF, no dots. Isolates escape handling + the 32K-entry LUT
/// compaction path — both engines use the same LUT, so expect near-parity.
fn esc_only_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(DECODED_TARGET + DECODED_TARGET / 64);
    for idx in 0..DECODED_TARGET {
        if idx % 256 == 128 {
            body.push(b'=');
            body.push(0x40); // '=@' -> one decoded byte (0xD6)
        } else {
            body.push(0x40);
        }
    }
    body
}

/// M-phase fixture: `'@'` body with ~0.4% literal `.` bytes, no CRLF. Body dots
/// (never at line start here) are data, not stuffing. The specials mask flags
/// only `= \r \n`, so this should stay on the fast path — if weaver lags,
/// there is a hidden dot cost to find.
fn dots_body() -> Vec<u8> {
    let mut body = vec![0x40u8; DECODED_TARGET];
    let mut idx = 128usize;
    while idx < DECODED_TARGET {
        body[idx] = b'.';
        idx += 256;
    }
    body
}

fn benches(c: &mut Criterion) {
    let Some(rapidyenc) = Rapidyenc::load() else {
        eprintln!(
            "skipping rapidyenc parity bench; set WEAVER_RAPIDYENC_LIB to a rapidyenc shared \
             library to enable it"
        );
        return;
    };
    eprintln!(
        "rapidyenc kernels: decode={} crc={}",
        rapidyenc.decode_kernel, rapidyenc.crc_kernel
    );

    let fixtures: [(&str, Vec<u8>); 5] = [
        ("realshape", real_yenc_128col_body()),
        ("clean", clean_body()),
        ("crlf_only", crlf_only_body()),
        ("esc_only", esc_only_body()),
        ("dots_body", dots_body()),
    ];

    for (name, input) in &fixtures {
        let mut weaver_out = vec![0u8; input.len() + 64];
        let mut rapid_out = vec![0u8; input.len() + 64];

        // Per-fixture parity gate: identical decoded bytes between the engines.
        let weaver_written = decode_rapidyenc(input, &mut weaver_out).unwrap();
        let rapid_written = rapidyenc.decode(input, &mut rapid_out);
        assert_eq!(
            weaver_written, rapid_written,
            "{name} decoded length parity"
        );
        assert_eq!(
            &weaver_out[..weaver_written],
            &rapid_out[..rapid_written],
            "{name} decoded byte parity"
        );
        eprintln!(
            "parity ok [{name}]: {} encoded -> {} decoded",
            input.len(),
            weaver_written
        );

        c.bench_function(&format!("parity_weaver_decode_{name}"), |b| {
            b.iter(|| {
                black_box(decode_rapidyenc(black_box(input), &mut weaver_out).unwrap());
            });
        });
        c.bench_function(&format!("parity_rapidyenc_decode_{name}"), |b| {
            b.iter(|| {
                black_box(rapidyenc.decode(black_box(input), &mut rapid_out));
            });
        });
    }

    // CRC parity over the realshape decoded bytes (unchanged).
    let realshape = &fixtures[0].1;
    let mut weaver_out = vec![0u8; realshape.len() + 64];
    let written = decode_rapidyenc(realshape, &mut weaver_out).unwrap();
    let decoded = &weaver_out[..written];
    c.bench_function("parity_crc32fast_decoded", |b| {
        b.iter(|| {
            let mut hasher = Crc32::new();
            hasher.update(black_box(decoded));
            black_box(hasher.finalize());
        });
    });
    c.bench_function("parity_rapidyenc_crc_decoded", |b| {
        b.iter(|| {
            black_box(rapidyenc.crc(black_box(decoded)));
        });
    });
}

criterion_group!(rapidyenc_parity, benches);
criterion_main!(rapidyenc_parity);
