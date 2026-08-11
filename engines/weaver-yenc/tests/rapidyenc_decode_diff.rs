use std::error::Error;
use std::ffi::OsString;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use weaver_yenc::{
    RapidyencDecodeEnd, RapidyencDecodeState, decode_rapidyenc_ex, decode_rapidyenc_incremental,
};

const ORACLE_SOURCE: &str = r#"
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "rapidyenc.h"

namespace RapidYenc {
int cpu_supports_isa() { return 0; }
int cpu_supports_crc_isa() { return 0; }
bool cpu_supports_neon() { return false; }
bool cpu_supports_rvv() { return false; }
void decoder_set_sse2_funcs() {}
void decoder_set_ssse3_funcs() {}
void decoder_set_avx_funcs() {}
void decoder_set_avx2_funcs() {}
void decoder_set_vbmi2_funcs() {}
void decoder_set_neon_funcs() {}
void decoder_set_rvv_funcs() {}
extern const bool decoder_has_avx10 = false;
}

static int hex_value(char c) {
	if(c >= '0' && c <= '9') return c - '0';
	if(c >= 'a' && c <= 'f') return c - 'a' + 10;
	if(c >= 'A' && c <= 'F') return c - 'A' + 10;
	return -1;
}

static std::vector<unsigned char> from_hex(const std::string& hex) {
	std::vector<unsigned char> out;
	if(hex == "-") return out;
	if(hex.size() % 2 != 0) {
		std::cerr << "odd hex length\n";
		std::exit(2);
	}
	out.reserve(hex.size() / 2);
	for(size_t i = 0; i < hex.size(); i += 2) {
		int hi = hex_value(hex[i]);
		int lo = hex_value(hex[i + 1]);
		if(hi < 0 || lo < 0) {
			std::cerr << "bad hex\n";
			std::exit(2);
		}
		out.push_back(static_cast<unsigned char>((hi << 4) | lo));
	}
	return out;
}

static void print_hex(const unsigned char* bytes, size_t len) {
	static const char* digits = "0123456789abcdef";
	if(len == 0) {
		std::cout << "-";
		return;
	}
	for(size_t i = 0; i < len; ++i) {
		std::cout << digits[bytes[i] >> 4] << digits[bytes[i] & 0xf];
	}
}

int main() {
	std::string mode;
	while(std::cin >> mode) {
		int raw = 0;
		int state_id = 0;
		std::string hex;
		if(mode == "ex") {
			std::cin >> raw >> state_id >> hex;
			std::vector<unsigned char> src = from_hex(hex);
			std::vector<unsigned char> dest(src.size() + 128);
			RapidYencDecoderState state = static_cast<RapidYencDecoderState>(state_id);
			size_t written = rapidyenc_decode_ex(raw, src.data(), dest.data(), src.size(), &state);
			std::cout << "OK " << written << " " << src.size() << " "
				<< static_cast<int>(state) << " 0 ";
			print_hex(dest.data(), written);
			std::cout << "\n";
		} else if(mode == "inc") {
			std::cin >> state_id >> hex;
			std::vector<unsigned char> src = from_hex(hex);
			std::vector<unsigned char> dest(src.size() + 128);
			RapidYencDecoderState state = static_cast<RapidYencDecoderState>(state_id);
			const void* src_ptr = src.data();
			void* dest_ptr = dest.data();
			RapidYencDecoderEnd end =
				rapidyenc_decode_incremental(&src_ptr, &dest_ptr, src.size(), &state);
			size_t consumed = static_cast<const unsigned char*>(src_ptr) - src.data();
			size_t written = static_cast<unsigned char*>(dest_ptr) - dest.data();
			std::cout << "OK " << written << " " << consumed << " "
				<< static_cast<int>(state) << " " << static_cast<int>(end) << " ";
			print_hex(dest.data(), written);
			std::cout << "\n";
		} else {
			std::cout << "ERR unknown-mode\n";
			return 2;
		}
		std::cout.flush();
	}
	return 0;
}
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Observation {
    bytes: Vec<u8>,
    consumed: usize,
    state: RapidyencDecodeState,
    end: RapidyencDecodeEnd,
}

struct Oracle {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    temp_dir: PathBuf,
}

impl Oracle {
    fn new() -> Result<Option<Self>, Box<dyn Error>> {
        let Some(root) = rapidyenc_root() else {
            return Ok(None);
        };

        let temp_dir = std::env::temp_dir().join(format!(
            "weaver-yenc-rapidyenc-oracle-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&temp_dir)?;
        let source = temp_dir.join("oracle.cc");
        let binary = temp_dir.join("oracle");
        std::fs::write(&source, ORACLE_SOURCE)?;

        let cxx = std::env::var_os("CXX").unwrap_or_else(|| OsString::from("c++"));
        let output = Command::new(cxx)
            .arg("-std=c++17")
            .arg("-O2")
            .arg("-DRAPIDYENC_DISABLE_ENCODE")
            .arg("-DRAPIDYENC_DISABLE_CRC")
            .arg("-I")
            .arg(&root)
            .arg(&source)
            .arg(root.join("rapidyenc.cc"))
            .arg(root.join("src/decoder.cc"))
            .arg("-o")
            .arg(&binary)
            .output()?;
        assert!(
            output.status.success(),
            "failed to build rapidyenc oracle\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let mut child = Command::new(&binary)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let stdin = child.stdin.take().expect("oracle stdin");
        let stdout = BufReader::new(child.stdout.take().expect("oracle stdout"));

        Ok(Some(Self {
            child,
            stdin,
            stdout,
            temp_dir,
        }))
    }

    fn decode_ex(
        &mut self,
        is_raw: bool,
        state: RapidyencDecodeState,
        input: &[u8],
    ) -> Result<Observation, Box<dyn Error>> {
        writeln!(
            self.stdin,
            "ex {} {} {}",
            usize::from(is_raw),
            state_id(state),
            hex_encode(input)
        )?;
        self.stdin.flush()?;
        self.read_observation()
    }

    fn decode_incremental(
        &mut self,
        state: RapidyencDecodeState,
        input: &[u8],
    ) -> Result<Observation, Box<dyn Error>> {
        writeln!(self.stdin, "inc {} {}", state_id(state), hex_encode(input))?;
        self.stdin.flush()?;
        self.read_observation()
    }

    fn read_observation(&mut self) -> Result<Observation, Box<dyn Error>> {
        let mut line = String::new();
        let n = self.stdout.read_line(&mut line)?;
        assert!(n > 0, "rapidyenc oracle exited before responding");
        let parts: Vec<_> = line.split_whitespace().collect();
        assert_eq!(parts.first(), Some(&"OK"), "oracle error: {line}");
        assert_eq!(parts.len(), 6, "bad oracle response: {line}");
        Ok(Observation {
            bytes: hex_decode(parts[5])?,
            consumed: parts[2].parse()?,
            state: state_from_id(parts[3].parse()?),
            end: end_from_id(parts[4].parse()?),
        })
    }
}

impl Drop for Oracle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.temp_dir);
    }
}

#[test]
fn rapidyenc_decode_ex_matches_local_oracle() -> Result<(), Box<dyn Error>> {
    let Some(mut oracle) = Oracle::new()? else {
        return Ok(());
    };
    let mut cases = fixed_cases();
    cases.extend(random_cases(0xdec0_de0d, 160));
    // Long enough for the flat SIMD kernels to engage (everything above stays
    // under the 128-byte gate, so the C oracle never saw a SIMD window).
    cases.extend(simd_fixed_cases());
    cases.extend(simd_random_cases(0x51d0_0d1e_5eed_1234, 200));

    let mut checked = 0usize;
    for case in &cases {
        for &is_raw in &[false, true] {
            for &state in states() {
                let expected = oracle.decode_ex(is_raw, state, case)?;
                let actual = weaver_decode_ex(is_raw, state, case)?;
                assert_eq!(
                    actual,
                    expected,
                    "decode_ex mismatch raw={is_raw} state={state:?} input={}",
                    hex_encode(case)
                );
                checked += 1;
            }
        }
    }

    eprintln!("rapidyenc decode_ex differential cases: {checked}");
    assert!(checked > 0);
    Ok(())
}

#[test]
fn rapidyenc_incremental_matches_local_oracle() -> Result<(), Box<dyn Error>> {
    let Some(mut oracle) = Oracle::new()? else {
        return Ok(());
    };
    let mut cases = fixed_cases();
    cases.extend(random_cases(0x1ced_cafe, 160));
    // Same SIMD-reaching corpus, but through the end-detecting entry point:
    // this family pins `consumed` exactly, not just the decoded bytes.
    cases.extend(simd_fixed_cases());
    cases.extend(simd_random_cases(0x0ff1_ce5e_c0de_7777, 200));

    let mut checked = 0usize;
    for case in &cases {
        for &state in states() {
            let expected = oracle.decode_incremental(state, case)?;
            let actual = weaver_decode_incremental(state, case)?;
            assert_eq!(
                actual,
                expected,
                "incremental mismatch state={state:?} input={}",
                hex_encode(case)
            );
            checked += 1;
        }
    }

    eprintln!("rapidyenc incremental differential cases: {checked}");
    assert!(checked > 0);
    Ok(())
}

#[test]
fn rapidyenc_chunk_boundaries_match_local_oracle() -> Result<(), Box<dyn Error>> {
    let Some(mut oracle) = Oracle::new()? else {
        return Ok(());
    };
    let cases = fixed_cases();
    let mut checked = 0usize;

    for case in &cases {
        for split in 0..=case.len() {
            let chunks = [&case[..split], &case[split..]];
            for &is_raw in &[false, true] {
                for &state in states() {
                    let expected = oracle_decode_ex_chunks(&mut oracle, is_raw, state, &chunks)?;
                    let actual = weaver_decode_ex_chunks(is_raw, state, &chunks)?;
                    assert_eq!(
                        actual,
                        expected,
                        "decode_ex chunk mismatch raw={is_raw} state={state:?} split={split} input={}",
                        hex_encode(case)
                    );
                    checked += 1;
                }
            }

            for &state in states() {
                let expected = oracle_incremental_chunks(&mut oracle, state, &chunks)?;
                let actual = weaver_incremental_chunks(state, &chunks)?;
                assert_eq!(
                    actual,
                    expected,
                    "incremental chunk mismatch state={state:?} split={split} input={}",
                    hex_encode(case)
                );
                checked += 1;
            }
        }

        let chunks: Vec<&[u8]> = case.chunks(1).collect();
        for &state in states() {
            let expected = oracle_incremental_chunks(&mut oracle, state, &chunks)?;
            let actual = weaver_incremental_chunks(state, &chunks)?;
            assert_eq!(
                actual,
                expected,
                "incremental bytewise mismatch state={state:?} input={}",
                hex_encode(case)
            );
            checked += 1;
        }
    }

    eprintln!("rapidyenc chunk-boundary differential cases: {checked}");
    assert!(checked > 0);
    Ok(())
}

/// The SIMD-reaching corpus across chunk splits.
///
/// Chunk splits are where the per-chunk `consumed` contract lives, and these
/// inputs are the first in this harness long enough for the flat SIMD kernels
/// to run at all. The split sweep is exhaustive for one ~600-byte case (plus a
/// byte-at-a-time pass, the strictest form of the contract) and sampled for the
/// longer cases — every 61st and 64th offset, which walks the split through all
/// residues of the 64-byte window, plus the first and last eight offsets —
/// which keeps the oracle round-trips bounded.
#[test]
fn rapidyenc_simd_chunk_boundaries_match_local_oracle() -> Result<(), Box<dyn Error>> {
    let Some(mut oracle) = Oracle::new()? else {
        return Ok(());
    };

    let sweep_case = simd_chunk_sweep_case();
    let mut plans: Vec<(Vec<u8>, Vec<usize>)> =
        vec![(sweep_case.clone(), (0..=sweep_case.len()).collect())];
    for case in simd_fixed_cases() {
        let splits = sparse_split_offsets(case.len());
        plans.push((case, splits));
    }

    let mut checked = 0usize;
    for (case, splits) in &plans {
        for &split in splits {
            let chunks = [&case[..split], &case[split..]];
            for &is_raw in &[false, true] {
                for &state in states() {
                    let expected = oracle_decode_ex_chunks(&mut oracle, is_raw, state, &chunks)?;
                    let actual = weaver_decode_ex_chunks(is_raw, state, &chunks)?;
                    assert_eq!(
                        actual,
                        expected,
                        "simd decode_ex chunk mismatch raw={is_raw} state={state:?} split={split} input={}",
                        hex_encode(case)
                    );
                    checked += 1;
                }
            }

            for &state in states() {
                let expected = oracle_incremental_chunks(&mut oracle, state, &chunks)?;
                let actual = weaver_incremental_chunks(state, &chunks)?;
                assert_eq!(
                    actual,
                    expected,
                    "simd incremental chunk mismatch state={state:?} split={split} input={}",
                    hex_encode(case)
                );
                checked += 1;
            }
        }
    }

    let chunks: Vec<&[u8]> = sweep_case.chunks(1).collect();
    for &state in states() {
        let expected = oracle_incremental_chunks(&mut oracle, state, &chunks)?;
        let actual = weaver_incremental_chunks(state, &chunks)?;
        assert_eq!(
            actual,
            expected,
            "simd incremental bytewise mismatch state={state:?} input={}",
            hex_encode(&sweep_case)
        );
        checked += 1;
    }

    eprintln!("rapidyenc SIMD chunk-boundary differential cases: {checked}");
    assert!(checked > 0);
    Ok(())
}

fn rapidyenc_root() -> Option<PathBuf> {
    let Some(root) = std::env::var_os("RAPIDYENC_ROOT").map(PathBuf::from) else {
        eprintln!("skipping rapidyenc differential tests; RAPIDYENC_ROOT is not set");
        return None;
    };
    if root.join("rapidyenc.cc").is_file() && root.join("src/decoder.cc").is_file() {
        Some(root)
    } else {
        eprintln!(
            "skipping rapidyenc differential tests; no rapidyenc checkout at {}",
            root.display()
        );
        None
    }
}

fn oracle_decode_ex_chunks(
    oracle: &mut Oracle,
    is_raw: bool,
    mut state: RapidyencDecodeState,
    chunks: &[&[u8]],
) -> Result<Observation, Box<dyn Error>> {
    let mut bytes = Vec::new();
    let mut consumed = 0usize;
    for chunk in chunks {
        let observed = oracle.decode_ex(is_raw, state, chunk)?;
        bytes.extend_from_slice(&observed.bytes);
        consumed += observed.consumed;
        state = observed.state;
    }
    Ok(Observation {
        bytes,
        consumed,
        state,
        end: RapidyencDecodeEnd::None,
    })
}

fn weaver_decode_ex_chunks(
    is_raw: bool,
    mut state: RapidyencDecodeState,
    chunks: &[&[u8]],
) -> Result<Observation, Box<dyn Error>> {
    let mut bytes = Vec::new();
    let mut consumed = 0usize;
    for chunk in chunks {
        let mut output = vec![0u8; chunk.len() + 128];
        let written = decode_rapidyenc_ex(is_raw, chunk, &mut output, &mut state)?;
        bytes.extend_from_slice(&output[..written]);
        consumed += chunk.len();
    }
    Ok(Observation {
        bytes,
        consumed,
        state,
        end: RapidyencDecodeEnd::None,
    })
}

fn oracle_incremental_chunks(
    oracle: &mut Oracle,
    mut state: RapidyencDecodeState,
    chunks: &[&[u8]],
) -> Result<Observation, Box<dyn Error>> {
    let mut bytes = Vec::new();
    let mut consumed = 0usize;
    let mut end = RapidyencDecodeEnd::None;
    for chunk in chunks {
        let observed = oracle.decode_incremental(state, chunk)?;
        bytes.extend_from_slice(&observed.bytes);
        consumed += observed.consumed;
        state = observed.state;
        end = observed.end;
        if end != RapidyencDecodeEnd::None {
            break;
        }
    }
    Ok(Observation {
        bytes,
        consumed,
        state,
        end,
    })
}

fn weaver_incremental_chunks(
    mut state: RapidyencDecodeState,
    chunks: &[&[u8]],
) -> Result<Observation, Box<dyn Error>> {
    let mut bytes = Vec::new();
    let mut consumed = 0usize;
    let mut end = RapidyencDecodeEnd::None;
    for chunk in chunks {
        let mut output = vec![0u8; chunk.len() + 128];
        let progress = decode_rapidyenc_incremental(chunk, &mut output, &mut state)?;
        bytes.extend_from_slice(&output[..progress.bytes_written]);
        consumed += progress.source_consumed;
        end = progress.end;
        if end != RapidyencDecodeEnd::None {
            break;
        }
    }
    Ok(Observation {
        bytes,
        consumed,
        state,
        end,
    })
}

fn weaver_decode_ex(
    is_raw: bool,
    state: RapidyencDecodeState,
    input: &[u8],
) -> Result<Observation, Box<dyn Error>> {
    let mut state = state;
    let mut output = vec![0u8; input.len() + 128];
    let written = decode_rapidyenc_ex(is_raw, input, &mut output, &mut state)?;
    Ok(Observation {
        bytes: output[..written].to_vec(),
        consumed: input.len(),
        state,
        end: RapidyencDecodeEnd::None,
    })
}

fn weaver_decode_incremental(
    state: RapidyencDecodeState,
    input: &[u8],
) -> Result<Observation, Box<dyn Error>> {
    let mut state = state;
    let mut output = vec![0u8; input.len() + 128];
    let progress = decode_rapidyenc_incremental(input, &mut output, &mut state)?;
    Ok(Observation {
        bytes: output[..progress.bytes_written].to_vec(),
        consumed: progress.source_consumed,
        state,
        end: progress.end,
    })
}

fn fixed_cases() -> Vec<Vec<u8>> {
    [
        b"".as_slice(),
        b"A",
        b"AB",
        b"=",
        b"=A",
        b"=\r",
        b"\r",
        b"\n",
        b"\r\n",
        b".",
        b"..",
        b"\r\n.",
        b"\r\n.\r",
        b"\r\n.\r\n",
        b"\r\n=y",
        b"\r\n=yignored",
        b"\r\n.=y",
        b"\r\n.=yignored",
        b"AB\r\n.\r\nEF",
        b"AB\r\n..CD",
        b"AB\r\n..CD\r\n.EF",
        b"AB=\r\n..CD\r\n.EF",
        b"AB\r\n=CC",
        b"AB\r\n.=CC",
        b"AB\r\n.\nEF",
        b"AB\r\n.\rEF",
        b"AB\r\n.=nCD",
        b"AB\r\n..=nCD",
    ]
    .into_iter()
    .map(<[u8]>::to_vec)
    .collect()
}

fn random_cases(mut seed: u64, count: usize) -> Vec<Vec<u8>> {
    const YENCISH: &[u8] = b"\r\n.=yABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut cases = Vec::with_capacity(count);
    for i in 0..count {
        seed = lcg(seed);
        let len = ((seed >> 32) as usize + i) % 96;
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            seed = lcg(seed);
            let pick = (seed >> 56) as usize;
            let byte = if pick.is_multiple_of(5) {
                (seed >> 24) as u8
            } else {
                YENCISH[pick % YENCISH.len()]
            };
            bytes.push(byte);
        }
        cases.push(bytes);
    }
    cases
}

fn lcg(seed: u64) -> u64 {
    seed.wrapping_mul(6364136223846793005).wrapping_add(1)
}

/// Special-free, line-structured body: `columns` data bytes per line separated
/// by `\r\n`, containing no `=`, `.`, CR or LF outside those breaks — so the
/// only escape or terminator in a case is the one the case splices in.
fn line_structured_body(len: usize, columns: usize) -> Vec<u8> {
    const DATA: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/-*";
    let mut body = Vec::with_capacity(len + 2);
    let mut col = 0usize;
    let mut idx = 0usize;
    while body.len() < len {
        if col == columns {
            body.extend_from_slice(b"\r\n");
            col = 0;
            continue;
        }
        body.push(DATA[idx % DATA.len()]);
        col += 1;
        idx += 1;
    }
    body.truncate(len);
    body
}

fn splice_at(body: &[u8], at: usize, seq: &[u8]) -> Vec<u8> {
    assert!(at + seq.len() <= body.len(), "splice past end of body");
    let mut out = body.to_vec();
    out[at..at + seq.len()].copy_from_slice(seq);
    out
}

/// The ~600-byte clean article body reserved for the exhaustive chunk-split
/// sweep: several 64-byte windows of 128-column data closed by a real trailer.
fn simd_chunk_sweep_case() -> Vec<u8> {
    let mut case = line_structured_body(560, 128);
    case.extend_from_slice(b"\r\n=yend size=560 part=1 pcrc32=1a2b3c4d");
    case
}

/// Fixed cases past the 128-byte flat-kernel gate: every one spans several
/// 64-byte SIMD windows, so these are the first inputs in this harness that
/// make the C oracle validate a weaver SIMD loop at all.
///
/// The window-edge families sweep the spliced sequence across absolute offsets
/// 254..=258 — bytes 62, 63, 64, 65 and 66 of the window that starts at 192 —
/// so the sequence starts inside one window, exactly on the edge, and inside
/// the next.
fn simd_fixed_cases() -> Vec<Vec<u8>> {
    /// Absolute offsets placing a spliced sequence at bytes 62..=66 relative to
    /// the 64-byte window starting at 192.
    const WINDOW_EDGE: [usize; 5] = [254, 255, 256, 257, 258];
    let base = line_structured_body(512, 128);
    let mut cases: Vec<Vec<u8>> = Vec::new();

    // (a) Clean multi-line body closed by a well-formed `=yend` trailer.
    let mut case = line_structured_body(384, 128);
    case.extend_from_slice(b"\r\n=yend size=384 part=2 pcrc32=deadbeef");
    cases.push(case);

    // (b) `\r\n=y` control terminator, (c) `\r\n.\r\n` article end and
    // (d) the dot-stuffed `\r\n.=y` control form, each swept across the edge.
    for seq in [b"\r\n=y".as_slice(), b"\r\n.\r\n", b"\r\n.=y"] {
        for at in WINDOW_EDGE {
            cases.push(splice_at(&base, at, seq));
        }
    }

    // (e) `=y` INSIDE a data line is an escaped `y`, never a boundary: the two
    // leading data bytes keep the `=` off a line start whatever surrounds it.
    for at in WINDOW_EDGE {
        cases.push(splice_at(&base, at, b"QQ=yQQ"));
    }

    // (f) Escape as the last byte of a window with the terminator opening the
    // next one, so the carried `escFirst` decides how the `\r` is read.
    for seq in [b"\r\n=y".as_slice(), b"\r\n.\r\n", b"\r\n.=y"] {
        for boundary in [256usize, 320] {
            let mut case = splice_at(&base, boundary, seq);
            case[boundary - 1] = b'=';
            cases.push(case);
        }
    }

    // (g) Dot-stuffed line starts mid-body, on and off the window edge.
    for at in [128usize, 190, 254, 255, 256, 257] {
        cases.push(splice_at(&base, at, b"\r\n..x"));
    }

    // (h) Dense escape runs straddling the window edge.
    for at in [250usize, 254, 255, 256] {
        cases.push(splice_at(&base, at, b"========"));
    }
    cases.push(splice_at(&base, 60, b"=========================="));

    // (i) A ~4 KiB realistic 128-column body with a trailer.
    let mut case = line_structured_body(4096, 128);
    case.extend_from_slice(b"\r\n=yend size=4096 part=3 pcrc32=0badc0de");
    cases.push(case);

    cases
}

/// Deterministic random cases sized for the SIMD loops: lengths 129..=4096,
/// three in four biased to within ±4 of a 64-byte window boundary (including
/// the 129-byte gate edge), with the same byte-class mix as [`random_cases`] —
/// mostly yEnc-significant bytes, one in five an arbitrary byte that can be NUL.
fn simd_random_cases(mut seed: u64, count: usize) -> Vec<Vec<u8>> {
    const YENCISH: &[u8] = b"\r\n.=yABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut cases = Vec::with_capacity(count);
    for _ in 0..count {
        seed = lcg(seed);
        let windows = 2 + ((seed >> 32) as usize % 63);
        seed = lcg(seed);
        let delta = ((seed >> 32) as i64 % 9) - 4;
        seed = lcg(seed);
        let len = if (seed >> 40).is_multiple_of(4) {
            129 + ((seed >> 32) as usize % 3968)
        } else {
            ((windows * 64) as i64 + delta).clamp(129, 4096) as usize
        };
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            seed = lcg(seed);
            let pick = (seed >> 56) as usize;
            let byte = if pick.is_multiple_of(5) {
                (seed >> 24) as u8
            } else {
                YENCISH[pick % YENCISH.len()]
            };
            bytes.push(byte);
        }
        cases.push(bytes);
    }
    cases
}

/// Split offsets for the long chunk-boundary cases: every 61st and 64th offset
/// (co-prime strides that walk the split through every residue of the 64-byte
/// window) plus the first and last eight offsets, where the pending-state
/// carries live.
fn sparse_split_offsets(len: usize) -> Vec<usize> {
    let mut offsets: Vec<usize> = (0..=len).step_by(61).collect();
    offsets.extend((0..=len).step_by(64));
    for back in 0..=8usize {
        offsets.push(back.min(len));
        offsets.push(len.saturating_sub(back));
    }
    offsets.sort_unstable();
    offsets.dedup();
    offsets
}

fn states() -> &'static [RapidyencDecodeState] {
    &[
        RapidyencDecodeState::CrLf,
        RapidyencDecodeState::Eq,
        RapidyencDecodeState::Cr,
        RapidyencDecodeState::None,
        RapidyencDecodeState::CrLfDot,
        RapidyencDecodeState::CrLfDotCr,
        RapidyencDecodeState::CrLfEq,
    ]
}

fn state_id(state: RapidyencDecodeState) -> usize {
    match state {
        RapidyencDecodeState::CrLf => 0,
        RapidyencDecodeState::Eq => 1,
        RapidyencDecodeState::Cr => 2,
        RapidyencDecodeState::None => 3,
        RapidyencDecodeState::CrLfDot => 4,
        RapidyencDecodeState::CrLfDotCr => 5,
        RapidyencDecodeState::CrLfEq => 6,
    }
}

fn state_from_id(id: usize) -> RapidyencDecodeState {
    match id {
        0 => RapidyencDecodeState::CrLf,
        1 => RapidyencDecodeState::Eq,
        2 => RapidyencDecodeState::Cr,
        3 => RapidyencDecodeState::None,
        4 => RapidyencDecodeState::CrLfDot,
        5 => RapidyencDecodeState::CrLfDotCr,
        6 => RapidyencDecodeState::CrLfEq,
        _ => panic!("unknown rapidyenc state id {id}"),
    }
}

fn end_from_id(id: usize) -> RapidyencDecodeEnd {
    match id {
        0 => RapidyencDecodeEnd::None,
        1 => RapidyencDecodeEnd::Control,
        2 => RapidyencDecodeEnd::Article,
        _ => panic!("unknown rapidyenc end id {id}"),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    if bytes.is_empty() {
        return "-".to_string();
    }
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    out
}

fn hex_decode(hex: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    if hex == "-" {
        return Ok(Vec::new());
    }
    assert_eq!(hex.len() % 2, 0, "odd hex length from oracle: {hex}");
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let hi = hex_nibble(pair[0])?;
        let lo = hex_nibble(pair[1])?;
        bytes.push((hi << 4) | lo);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Result<u8, Box<dyn Error>> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("bad hex nibble {}", byte as char).into()),
    }
}
