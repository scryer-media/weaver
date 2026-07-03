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

    let input = real_yenc_128col_body();
    let mut weaver_out = vec![0u8; input.len() + 64];
    let mut rapid_out = vec![0u8; input.len() + 64];

    // Parity gate: identical decoded bytes and identical CRC over them.
    let weaver_written = decode_rapidyenc(&input, &mut weaver_out).unwrap();
    let rapid_written = rapidyenc.decode(&input, &mut rapid_out);
    assert_eq!(weaver_written, rapid_written, "decoded length parity");
    assert_eq!(
        &weaver_out[..weaver_written],
        &rapid_out[..rapid_written],
        "decoded byte parity"
    );
    let mut hasher = Crc32::new();
    hasher.update(&weaver_out[..weaver_written]);
    let weaver_crc = hasher.finalize();
    let rapid_crc = rapidyenc.crc(&rapid_out[..rapid_written]);
    assert_eq!(weaver_crc, rapid_crc, "crc parity");
    eprintln!(
        "parity ok: {} encoded -> {} decoded, crc {weaver_crc:08x}",
        input.len(),
        weaver_written
    );

    c.bench_function("parity_weaver_decode_realshape", |b| {
        b.iter(|| {
            let written = decode_rapidyenc(black_box(&input), &mut weaver_out).unwrap();
            black_box(written);
        });
    });
    c.bench_function("parity_rapidyenc_decode_realshape", |b| {
        b.iter(|| {
            let written = rapidyenc.decode(black_box(&input), &mut rapid_out);
            black_box(written);
        });
    });

    let decoded = &weaver_out[..weaver_written];
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
