//! Per-stage timing of the whole-buffer article decode (`decode_nntp`), the
//! path the download pipeline runs for every yEnc article.
//!
//! Splits one article into its stages (header and trailer location, field
//! parsing, body decode, CRC) and times each in isolation, interleaved, so the
//! difference between the article total and the body decode is attributed.
//!
//!   cargo run --release --example article_profile
//!   cargo run --release --example article_profile -- mt <threads> <iters> [rapidyenc]
//!   cargo run --release --example article_profile -- kernels
//!
//! With `WEAVER_RAPIDYENC_SRC` set, a rapidyenc lane times the same article
//! driven the way sabctools drives rapidyenc, as a comparison point.

use std::hint::black_box;
use std::time::Instant;

use weaver_yenc::crc::Crc32;
use weaver_yenc::header::{
    apply_ypart_line, parse_headers_with_options, parse_ybegin_line, parse_yend_line,
};
use weaver_yenc::{
    DecodeOptions, RapidyencDecodeState, decode_body, decode_nntp, decode_rapidyenc,
    decode_rapidyenc_incremental, encode_part, max_decoded_len,
};

const PART: usize = 768_000;

fn article() -> Vec<u8> {
    let data: Vec<u8> = (0..PART).map(|i| ((i * 31 + 17) & 0xff) as u8).collect();
    let mut out = Vec::with_capacity(PART + PART / 32 + 512);
    let begin = 3 * PART as u64 + 1;
    encode_part(
        &data,
        &mut out,
        128,
        "invented.part07.rar",
        4,
        40,
        begin,
        begin + PART as u64 - 1,
        40 * PART as u64,
    )
    .unwrap();
    out
}

fn lines(article: &[u8]) -> (usize, usize, usize, usize) {
    let lf = |from: usize| from + memchr::memchr(b'\n', &article[from..]).unwrap() + 1;
    let after_ybegin = lf(0);
    let after_ypart = lf(after_ybegin);
    let yend = memchr::memmem::rfind(article, b"\r\n=yend").unwrap() + 2;
    (after_ybegin, after_ypart, yend, article.len())
}

fn time(iters: usize, f: &mut dyn FnMut()) -> f64 {
    let t = Instant::now();
    for _ in 0..iters {
        f();
    }
    t.elapsed().as_nanos() as f64 / iters as f64 / 1000.0
}

fn med(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// A named timed closure and its per-round results.
type Stage = (&'static str, Box<dyn FnMut()>, Vec<f64>);

fn per_stage() {
    let art = article();
    let (after_ybegin, after_ypart, yend, end) = lines(&art);
    let ybegin_line = &art[..after_ybegin - 2];
    let ypart_line = &art[after_ybegin..after_ypart - 2];
    let yend_line = &art[yend..end - 2];
    let body = &art[after_ypart..yend - 2];
    let nntp = DecodeOptions {
        dot_unstuffing: true,
    };
    let mut out = vec![0u8; max_decoded_len(art.len())];
    let written = decode_body(body, &mut out, &mut Crc32::new(), nntp).unwrap();

    let rounds: usize = std::env::var("ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9);
    let iters = 400;
    let mut stages: Vec<Stage> = Vec::new();
    {
        let art = art.clone();
        let mut out = out.clone();
        stages.push((
            "article total (decode_nntp)",
            Box::new(move || {
                let r = decode_nntp(black_box(&art), &mut out).unwrap();
                black_box(r.part_crc);
            }),
            vec![],
        ));
    }
    {
        let art = art.clone();
        stages.push((
            "parse_headers (line-scan path)",
            Box::new(move || {
                black_box(parse_headers_with_options(black_box(&art), nntp).unwrap());
            }),
            vec![],
        ));
    }
    {
        let l = ybegin_line.to_vec();
        stages.push((
            "  =ybegin field parse",
            Box::new(move || {
                black_box(parse_ybegin_line(black_box(&l)).unwrap());
            }),
            vec![],
        ));
    }
    {
        let lb = ybegin_line.to_vec();
        let lp = ypart_line.to_vec();
        let mut md = parse_ybegin_line(&lb).unwrap();
        stages.push((
            "  =ypart field parse",
            Box::new(move || {
                apply_ypart_line(black_box(&lp), &mut md).unwrap();
                black_box(&md);
            }),
            vec![],
        ));
    }
    {
        let l = yend_line.to_vec();
        stages.push((
            "  =yend field parse",
            Box::new(move || {
                black_box(parse_yend_line(black_box(&l)).unwrap());
            }),
            vec![],
        ));
    }
    {
        let art = art.clone();
        let from = after_ypart;
        stages.push((
            "  body LF walk (trailer search model)",
            Box::new(move || {
                let input = black_box(&art[..]);
                let mut pos = from;
                let mut hit = 0;
                while let Some(rel) = memchr::memchr(b'\n', &input[pos..]) {
                    let lf = pos + rel;
                    if input[lf - 1] == b'\r' && input[lf + 1..].starts_with(b"=y") {
                        hit = lf;
                        break;
                    }
                    pos = lf + 1;
                }
                black_box(hit);
            }),
            vec![],
        ));
    }
    {
        let art = art.clone();
        let from = after_ypart;
        let finder = memchr::memmem::Finder::new(b"=y").into_owned();
        stages.push((
            "  body =y memmem (trailer search alt)",
            Box::new(move || {
                let input = black_box(&art[..]);
                let mut hit = 0;
                for q in finder.find_iter(&input[from..]) {
                    let q = from + q;
                    if q >= from + 2 && &input[q - 2..q] == b"\r\n" {
                        hit = q;
                        break;
                    }
                }
                black_box(hit);
            }),
            vec![],
        ));
    }
    {
        let art = art.clone();
        let from = after_ypart;
        let finder = memchr::memmem::Finder::new(b"\r\n=y").into_owned();
        stages.push((
            "  body \\r\\n=y memmem",
            Box::new(move || {
                black_box(finder.find(black_box(&art[from..])));
            }),
            vec![],
        ));
    }
    {
        let b = body.to_vec();
        let mut o = out.clone();
        stages.push((
            "  kernel raw, no end search",
            Box::new(move || {
                black_box(decode_rapidyenc(black_box(&b), &mut o).unwrap());
            }),
            vec![],
        ));
    }
    {
        let b = art[after_ypart..].to_vec();
        let mut o = out.clone();
        stages.push((
            "  kernel raw, end search (to =yend)",
            Box::new(move || {
                let mut st = RapidyencDecodeState::CrLf;
                black_box(decode_rapidyenc_incremental(black_box(&b), &mut o, &mut st).unwrap());
            }),
            vec![],
        ));
    }
    {
        let b = body.to_vec();
        let mut o = out.clone();
        stages.push((
            "body decode + CRC (decode_body)",
            Box::new(move || {
                let mut crc = Crc32::new();
                black_box(decode_body(black_box(&b), &mut o, &mut crc, nntp).unwrap());
                black_box(crc.finalize());
            }),
            vec![],
        ));
    }
    {
        let d = out[..written].to_vec();
        stages.push((
            "  CRC alone",
            Box::new(move || {
                let mut crc = Crc32::new();
                crc.update(black_box(&d));
                black_box(crc.finalize());
            }),
            vec![],
        ));
    }

    #[cfg(rapidyenc_linked)]
    {
        unsafe {
            weaver_rapidyenc_decode_init();
            weaver_rapidyenc_crc32_init();
        }
        let art = art.clone();
        let mut o = out.clone();
        stages.push((
            "rapidyenc article (SAB shape)",
            Box::new(move || {
                black_box(rapidyenc_article(black_box(&art), &mut o));
            }),
            vec![],
        ));
    }

    for r in 0..rounds {
        let n = stages.len();
        for k in 0..n {
            let idx = if r % 2 == 0 { k } else { n - 1 - k };
            let (_, f, v) = &mut stages[idx];
            f();
            v.push(time(iters, f.as_mut()));
        }
    }
    println!(
        "article {} B, body {} B, decoded {} B, {} lines",
        art.len(),
        body.len(),
        written,
        memchr::memchr_iter(b'\n', &art).count()
    );
    for (name, _, v) in stages {
        println!("{name:<40} {:>9.2} us", med(v));
    }
}

#[cfg(rapidyenc_linked)]
unsafe extern "C" {
    fn weaver_rapidyenc_decode_init();
    fn weaver_rapidyenc_crc32_init();
    fn weaver_rapidyenc_decode(
        src: *const core::ffi::c_void,
        dest: *mut core::ffi::c_void,
        len: u64,
    ) -> u64;
    fn weaver_rapidyenc_crc32(data: *const core::ffi::c_void, len: u64, init: u32) -> u32;
    fn weaver_rapidyenc_decode_end(
        src: *const core::ffi::c_void,
        dest: *mut core::ffi::c_void,
        len: u64,
        consumed: *mut u64,
        written: *mut u64,
    ) -> i32;
}

/// One article the way sabctools drives rapidyenc: header lines parsed one at
/// a time by substring search until the body starts, the body decoded in
/// 64 KiB chunks by the end-detecting decoder with the CRC folded per chunk,
/// then the `=yend` line parsed where the decoder stopped. Returns
/// (bytes written, crc, crc expected) so nothing is optimised away.
#[cfg(rapidyenc_linked)]
fn rapidyenc_article(art: &[u8], out: &mut [u8]) -> (usize, u32, u32) {
    fn field(line: &[u8], key: &[u8]) -> Option<u64> {
        let at = memchr::memmem::find(line, key)? + key.len();
        let digits = line[at..].iter().take_while(|b| b.is_ascii_digit());
        Some(digits.fold(0u64, |acc, &b| acc * 10 + u64::from(b - b'0')))
    }
    let mut pos = 0;
    let mut size = 0;
    loop {
        let lf = pos + memchr::memchr(b'\n', &art[pos..]).unwrap();
        let line = &art[pos..lf];
        pos = lf + 1;
        if line.starts_with(b"=ybegin ") {
            size = field(line, b" size=").unwrap_or(0);
            if field(line, b" part=").is_none() {
                break;
            }
        } else if line.starts_with(b"=ypart ") {
            black_box((field(line, b" begin="), field(line, b" end=")));
            break;
        }
    }
    let (mut crc, mut written) = (0u32, 0usize);
    loop {
        let chunk = (art.len() - pos).min(64 * 1024);
        let (mut consumed, mut produced) = (0u64, 0u64);
        let end = unsafe {
            weaver_rapidyenc_decode_end(
                art[pos..].as_ptr().cast(),
                out[written..].as_mut_ptr().cast(),
                chunk as u64,
                &mut consumed,
                &mut produced,
            )
        };
        crc = unsafe { weaver_rapidyenc_crc32(out[written..].as_ptr().cast(), produced, crc) };
        pos += consumed as usize;
        written += produced as usize;
        if end != 0 || pos == art.len() {
            break;
        }
    }
    // Back up over the `=y` the decoder consumed and parse the trailer line.
    let line = &art[pos - 2..];
    let line = &line[..memchr::memchr(b'\n', line).unwrap_or(line.len())];
    let expected = memchr::memmem::find(line, b" pcrc32=")
        .map(|at| {
            line[at + 8..]
                .iter()
                .take_while(|b| b.is_ascii_hexdigit())
                .fold(0u32, |acc, &b| acc * 16 + (b as char).to_digit(16).unwrap())
        })
        .unwrap_or(0);
    black_box((size, field(line, b" size=")));
    (written, crc, expected)
}

/// The body decoders alone, with and without end detection, at several
/// alignments of the body start, so the end-detection cost is separated from
/// where the body happens to sit in the article buffer.
fn kernels() {
    let art = article();
    let (_, after_ypart, yend, _) = lines(&art);
    let rounds: usize = std::env::var("ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9);
    let iters = 400;
    #[cfg(rapidyenc_linked)]
    unsafe {
        weaver_rapidyenc_decode_init();
    }
    let mut stages: Vec<Stage> = Vec::new();
    for off in [0usize, 16, 32, 48, 61] {
        // A 64-byte aligned copy of the body (and its trailer) at `off`.
        let tail = &art[after_ypart..];
        let body_len = yend - 2 - after_ypart;
        let tail_len = tail.len();
        let mut backing = vec![0u8; tail.len() + 128];
        let base = backing.as_ptr().align_offset(64) + off;
        backing[base..base + tail.len()].copy_from_slice(tail);
        let backing = std::rc::Rc::new(backing);
        {
            // The whole article, placed so its body starts `off` bytes past a
            // 64-byte boundary.
            let mut whole = vec![0u8; art.len() + 128];
            let start = whole.as_ptr().align_offset(64) + (off + 64 - after_ypart % 64) % 64;
            whole[start..start + art.len()].copy_from_slice(&art);
            let len = art.len();
            let mut o = vec![0u8; max_decoded_len(len)];
            stages.push((
                Box::leak(format!("article decode_nntp @{off}").into_boxed_str()),
                Box::new(move || {
                    let r = decode_nntp(black_box(&whole[start..start + len]), &mut o).unwrap();
                    black_box(r.part_crc);
                }),
                vec![],
            ));
        }
        let label = |s: &str| -> &'static str { Box::leak(format!("{s} @{off}").into_boxed_str()) };
        {
            let b = backing.clone();
            let mut o = vec![0u8; tail.len() + 64];
            stages.push((
                label("weaver no end"),
                Box::new(move || {
                    black_box(
                        decode_rapidyenc(black_box(&b[base..base + body_len]), &mut o).unwrap(),
                    );
                }),
                vec![],
            ));
        }
        {
            let b = backing.clone();
            let mut o = vec![0u8; tail.len() + 64];
            stages.push((
                label("weaver end search"),
                Box::new(move || {
                    let mut st = RapidyencDecodeState::CrLf;
                    black_box(
                        decode_rapidyenc_incremental(
                            black_box(&b[base..base + tail_len]),
                            &mut o,
                            &mut st,
                        )
                        .unwrap(),
                    );
                }),
                vec![],
            ));
        }
        #[cfg(rapidyenc_linked)]
        {
            let b = backing.clone();
            let mut o = vec![0u8; tail.len() + 64];
            stages.push((
                label("rapidyenc no end"),
                Box::new(move || unsafe {
                    black_box(weaver_rapidyenc_decode(
                        b[base..].as_ptr().cast(),
                        o.as_mut_ptr().cast(),
                        body_len as u64,
                    ));
                }),
                vec![],
            ));
            let b = backing.clone();
            let mut o = vec![0u8; tail.len() + 64];
            let n = tail_len as u64;
            stages.push((
                label("rapidyenc end search"),
                Box::new(move || unsafe {
                    let (mut c, mut w) = (0u64, 0u64);
                    black_box(weaver_rapidyenc_decode_end(
                        b[base..].as_ptr().cast(),
                        o.as_mut_ptr().cast(),
                        n,
                        &mut c,
                        &mut w,
                    ));
                }),
                vec![],
            ));
        }
    }
    for r in 0..rounds {
        let n = stages.len();
        for k in 0..n {
            let idx = if r % 2 == 0 { k } else { n - 1 - k };
            let (_, f, v) = &mut stages[idx];
            f();
            v.push(time(iters, f.as_mut()));
        }
    }
    for (name, _, v) in stages {
        println!("{name:<32} {:>9.2} us", med(v));
    }
}

fn multi_thread(threads: usize, iters: usize, rapidyenc: bool) {
    #[cfg(rapidyenc_linked)]
    unsafe {
        weaver_rapidyenc_decode_init();
        weaver_rapidyenc_crc32_init();
    }
    #[cfg(not(rapidyenc_linked))]
    assert!(
        !rapidyenc,
        "build with WEAVER_RAPIDYENC_SRC for the rapidyenc lane"
    );
    let art = article();
    let t = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..threads {
            let art = &art;
            s.spawn(move || {
                let mut out = vec![0u8; max_decoded_len(art.len())];
                for _ in 0..iters {
                    #[cfg(rapidyenc_linked)]
                    if rapidyenc {
                        black_box(rapidyenc_article(black_box(art), &mut out));
                        continue;
                    }
                    let r = decode_nntp(black_box(art), &mut out).unwrap();
                    black_box(r.part_crc);
                }
            });
        }
    });
    let wall = t.elapsed().as_secs_f64();
    let bytes = (art.len() * iters * threads) as f64;
    let lane = if rapidyenc { "rapidyenc" } else { "weaver" };
    println!(
        "mt lane={lane} threads={threads} iters={iters} wall_s={wall:.4} GB/s={:.2}",
        bytes / wall / 1e9
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("mt") {
        let threads = args[2].parse().unwrap();
        let iters = args[3].parse().unwrap();
        let rapidyenc = args.get(4).map(String::as_str) == Some("rapidyenc");
        multi_thread(threads, iters, rapidyenc);
    } else if args.get(1).map(String::as_str) == Some("kernels") {
        kernels();
    } else {
        per_stage();
    }
}
