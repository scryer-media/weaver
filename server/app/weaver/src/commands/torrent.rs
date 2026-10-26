// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::Read;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use weaver_server_core::torrents::analysis::{
    TorrentReport, analyze_magnet_link, analyze_metainfo,
};

use crate::args::TorrentCommand;

// Largest .torrent file the analyzer will read.
const MAX_TORRENT_BYTES: u64 = 64 * 1024 * 1024;

// Exit code 0 when a report was printed, 1 when the file could not be read
// or the source is not a torrent.
pub(crate) fn run(command: TorrentCommand) -> i32 {
    match command {
        TorrentCommand::Analyze { source, json } => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs())
                .unwrap_or(0);
            match analyze(&source, json, now) {
                Ok(report) => {
                    println!("{report}");
                    0
                }
                Err(error) => {
                    // A magnet is not echoed back: it carries the infohash.
                    let shown = if is_magnet(&source) {
                        "magnet"
                    } else {
                        &source
                    };
                    eprintln!("weaver torrent analyze: {shown}: {error}");
                    1
                }
            }
        }
    }
}

fn is_magnet(source: &str) -> bool {
    source
        .get(..7)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("magnet:"))
}

fn analyze(source: &str, json: bool, now_epoch_secs: u64) -> Result<String, String> {
    let report = if is_magnet(source) {
        analyze_magnet_link(source, now_epoch_secs).map_err(|error| error.to_string())?
    } else {
        let bytes = read_torrent(Path::new(source), MAX_TORRENT_BYTES)?;
        analyze_metainfo(&bytes, now_epoch_secs).map_err(|error| error.to_string())?
    };
    Ok(render(report, json))
}

fn render(report: TorrentReport, json: bool) -> String {
    let report = report.with_weaver_version(env!("CARGO_PKG_VERSION"));
    if json {
        report.to_json()
    } else {
        report.to_text().trim_end().to_string()
    }
}

fn read_torrent(path: &Path, limit: u64) -> Result<Vec<u8>, String> {
    let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > limit {
        return Err(format!("torrent is larger than {limit} bytes"));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{analyze, is_magnet, read_torrent};

    const NOW: u64 = 1_790_000_000;

    fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
        out.extend_from_slice(bytes);
    }

    fn sample_torrent() -> Vec<u8> {
        let mut out = b"d".to_vec();
        bstr(&mut out, b"announce");
        bstr(
            &mut out,
            b"https://tracker.example/Quiet/announce?passkey=Harbor",
        );
        bstr(&mut out, b"info");
        out.push(b'd');
        bstr(&mut out, b"length");
        out.extend_from_slice(b"i40000e");
        bstr(&mut out, b"name");
        bstr(&mut out, b"Quiet.Harbor.mkv");
        bstr(&mut out, b"piece length");
        out.extend_from_slice(b"i16384e");
        bstr(&mut out, b"pieces");
        bstr(&mut out, &[1u8; 60]);
        out.extend_from_slice(b"ee");
        out
    }

    #[test]
    fn a_file_and_a_magnet_report_without_names() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Quiet.Harbor.torrent");
        std::fs::write(&path, sample_torrent()).unwrap();
        let text = analyze(path.to_str().unwrap(), false, NOW).unwrap();
        assert!(text.contains(env!("CARGO_PKG_VERSION")), "{text}");
        assert!(text.contains("tracker.example"), "{text}");
        for piece in ["Quiet", "Harbor", "passkey"] {
            assert!(!text.contains(piece), "{piece} leaked into:\n{text}");
        }
        let json = analyze(path.to_str().unwrap(), true, NOW).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(json["identity"]["source"], "metainfo");

        let magnet = format!("magnet:?xt=urn:btih:{}&dn=Quiet.Harbor", "ab".repeat(20));
        let text = analyze(&magnet, false, NOW).unwrap();
        assert!(!text.contains("Quiet"), "{text}");
        assert!(!text.contains(&"ab".repeat(20)), "{text}");
        assert!(is_magnet("MAGNET:?xt=urn:btih:00"));
    }

    #[test]
    fn a_file_that_is_not_a_torrent_or_is_too_large_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.torrent");
        std::fs::write(&path, b"not a torrent").unwrap();
        assert!(analyze(path.to_str().unwrap(), false, NOW).is_err());
        assert!(read_torrent(&path, 4).is_err());
        assert!(analyze(&dir.path().join("absent").display().to_string(), false, NOW).is_err());
    }
}
