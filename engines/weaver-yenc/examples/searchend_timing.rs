//! searchEnd (end-detection) tax decomposition harness.
//!
//! Same protocol as `decode_timing` (min of 2000 iters, in-process A/B against
//! the real rapidyenc when `WEAVER_RAPIDYENC_SRC` is set), but it times FOUR
//! lanes on the same fixture in the same run so the searchEnd tax can be
//! attributed:
//!
//!   decode_only   weaver `decode_rapidyenc`              (kernel, SEARCH_END=false, no CRC)
//!   until_end     weaver `decode_rapidyenc_incremental`  (kernel, SEARCH_END=true,  no CRC)
//!   bench_shape   weaver `decode_body_chunk_until_control` x3 chunks + finalize_crc
//!                 (exactly what benches/decode_simd.rs `bench_until_control` does)
//!   crc_only      `Crc32::update` over the decoded output, nothing else
//!
//! and, when linked, the rapidyenc equivalents of the first two lanes
//! (`decode` vs `decode_end`).
//!
//!   cargo run --release --example searchend_timing

use std::time::Instant;

use weaver_yenc::crc::Crc32;
use weaver_yenc::decode::{
    DecodeState, decode_body_chunk_until_control, decode_rapidyenc, decode_rapidyenc_incremental,
};

const DECODED_TARGET: usize = 768_000;
const ITERS: usize = 2000;

fn real_yenc_128col_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(800 * 1024);
    let mut col = 0usize;
    for idx in 0..DECODED_TARGET {
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

fn clean_body() -> Vec<u8> {
    vec![0x40u8; DECODED_TARGET]
}

fn dots_body() -> Vec<u8> {
    let mut body = vec![0x40u8; DECODED_TARGET];
    let mut idx = 128usize;
    while idx < DECODED_TARGET {
        body[idx] = b'.';
        idx += 256;
    }
    body
}

fn measure(mut f: impl FnMut()) -> (f64, f64) {
    for _ in 0..100 {
        f();
    }
    let mut samples = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t = Instant::now();
        f();
        samples.push(t.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();
    (
        samples[0] as f64 / 1000.0,
        samples[ITERS / 2] as f64 / 1000.0,
    )
}

fn row(label: &str, min: f64, median: f64, base: f64) {
    println!(
        "  {label:<26} min {min:>9.3} us  median {median:>9.3} us  ({:.3}x base)",
        min / base
    );
}

fn time_fixture(name: &str, input: &[u8]) {
    let mut out = vec![0u8; input.len() + 64];
    let decoded = decode_rapidyenc(input, &mut out).unwrap();
    println!("{name}  ({} encoded bytes, {decoded} decoded)", input.len());

    let (dec_min, dec_med) = measure(|| {
        let w = decode_rapidyenc(std::hint::black_box(input), &mut out).unwrap();
        std::hint::black_box(w);
    });
    row("weaver decode_only", dec_min, dec_med, dec_min);

    {
        let mut state = Default::default();
        let p = decode_rapidyenc_incremental(input, &mut out, &mut state).unwrap();
        println!(
            "    [validity] weaver until_end: consumed {} / {}, written {} / {decoded}, end {:?}",
            p.source_consumed,
            input.len(),
            p.bytes_written,
            p.end
        );
    }
    let (inc_min, inc_med) = measure(|| {
        let mut state = Default::default();
        let p = decode_rapidyenc_incremental(std::hint::black_box(input), &mut out, &mut state)
            .unwrap();
        std::hint::black_box(p.bytes_written);
    });
    row("weaver until_end", inc_min, inc_med, dec_min);

    // Exactly benches/decode_simd.rs::bench_until_control.
    let mut vout: Vec<u8> = Vec::with_capacity(input.len() + 64);
    let (bench_min, bench_med) = measure(|| {
        vout.clear();
        let mut state = DecodeState::new();
        let chunk_len = input.len().div_ceil(3);
        let mut start = 0usize;
        while start < input.len() {
            let end = (start + chunk_len).min(input.len());
            let progress = decode_body_chunk_until_control(
                &mut state,
                std::hint::black_box(&input[start..end]),
                &mut vout,
            )
            .unwrap();
            if progress.source_consumed == 0 {
                break;
            }
            start += progress.source_consumed;
        }
        std::hint::black_box(vout.len());
        std::hint::black_box(state.finalize_crc());
    });
    row("weaver bench_until_control", bench_min, bench_med, dec_min);

    let payload = out[..decoded].to_vec();
    let (crc_min, crc_med) = measure(|| {
        let mut crc = Crc32::new();
        crc.update(std::hint::black_box(&payload));
        std::hint::black_box(crc.finalize());
    });
    row("crc_only (over decoded)", crc_min, crc_med, dec_min);

    #[cfg(rapidyenc_linked)]
    {
        let mut rout = vec![0u8; input.len() + 64];
        let (rdec_min, rdec_med) = measure(|| unsafe {
            let w = weaver_rapidyenc_decode(
                std::hint::black_box(input.as_ptr()) as *const core::ffi::c_void,
                rout.as_mut_ptr() as *mut core::ffi::c_void,
                input.len() as u64,
            );
            std::hint::black_box(w);
        });
        row("rapidyenc decode_only", rdec_min, rdec_med, dec_min);

        unsafe {
            let mut consumed = 0u64;
            let mut written = 0u64;
            let e = weaver_rapidyenc_decode_end(
                input.as_ptr() as *const core::ffi::c_void,
                rout.as_mut_ptr() as *mut core::ffi::c_void,
                input.len() as u64,
                &mut consumed,
                &mut written,
            );
            println!(
                "    [validity] rapidyenc until_end: consumed {consumed} / {}, written {written} / {decoded}, end {e}",
                input.len()
            );
            assert_eq!(
                &rout[..written as usize],
                &out[..written as usize],
                "{name}: rapidyenc until_end bytes != weaver"
            );
        }
        let (rinc_min, rinc_med) = measure(|| unsafe {
            let mut consumed = 0u64;
            let mut written = 0u64;
            let e = weaver_rapidyenc_decode_end(
                std::hint::black_box(input.as_ptr()) as *const core::ffi::c_void,
                rout.as_mut_ptr() as *mut core::ffi::c_void,
                input.len() as u64,
                &mut consumed,
                &mut written,
            );
            std::hint::black_box((e, consumed, written));
        });
        row("rapidyenc until_end", rinc_min, rinc_med, dec_min);

        println!(
            "  >> kernel tax: weaver {:.3}x   rapidyenc {:.3}x  |  weaver/oracle: decode {:.3}x  until_end {:.3}x",
            inc_min / dec_min,
            rinc_min / rdec_min,
            dec_min / rdec_min,
            inc_min / rinc_min
        );
    }
    #[cfg(not(rapidyenc_linked))]
    println!(
        "  >> kernel tax: weaver {:.3}x  (set WEAVER_RAPIDYENC_SRC for the oracle lanes)",
        inc_min / dec_min
    );
    println!();
}

#[cfg(rapidyenc_linked)]
unsafe extern "C" {
    fn weaver_rapidyenc_decode_init();
    fn weaver_rapidyenc_decode(
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
}

fn main() {
    #[cfg(rapidyenc_linked)]
    unsafe {
        weaver_rapidyenc_decode_init();
    }
    println!("weaver-yenc searchEnd tax decomposition (min of {ITERS} iters)\n");
    time_fixture("realshape", &real_yenc_128col_body());
    time_fixture("crlf_only", &crlf_only_body());
    time_fixture("clean", &clean_body());
    time_fixture("dots_body", &dots_body());
}
