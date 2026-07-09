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
