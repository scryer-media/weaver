use std::io::Read;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::args::NzbCommand;

/// Largest NZB, after decompression, the analyzer will read.
const MAX_NZB_BYTES: u64 = 512 * 1024 * 1024;

const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];
const ZSTD_MAGIC: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd];

/// Exit code 0 when a report was printed, 1 when the file could not be read
/// or is not an NZB.
pub(crate) fn run(command: NzbCommand) -> i32 {
    match command {
        NzbCommand::Analyze { file, json } => match analyze_file(&file, json) {
            Ok(report) => {
                println!("{report}");
                0
            }
            Err(error) => {
                eprintln!("weaver nzb analyze: {}: {error}", file.display());
                1
            }
        },
    }
}

fn analyze_file(path: &Path, json: bool) -> Result<String, String> {
    let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    render(bytes, json, now)
}

/// The compression is read from the bytes, so a renamed file still opens.
fn decompress(bytes: Vec<u8>) -> Result<Vec<u8>, String> {
    let source = std::io::Cursor::new(bytes);
    let decoded: Box<dyn Read> = if source.get_ref().starts_with(GZIP_MAGIC) {
        Box::new(flate2::read::GzDecoder::new(source))
    } else if source.get_ref().starts_with(ZSTD_MAGIC) {
        Box::new(zstd::stream::read::Decoder::new(source).map_err(|error| error.to_string())?)
    } else {
        Box::new(source)
    };
    let mut xml = Vec::new();
    decoded
        .take(MAX_NZB_BYTES + 1)
        .read_to_end(&mut xml)
        .map_err(|error| error.to_string())?;
    if xml.len() as u64 > MAX_NZB_BYTES {
        return Err(format!("NZB is larger than {MAX_NZB_BYTES} bytes"));
    }
    Ok(xml)
}

fn render(bytes: Vec<u8>, json: bool, now_epoch_secs: u64) -> Result<String, String> {
    let xml = decompress(bytes)?;
    let (nzb, diagnostics) =
        weaver_nzb::parse_nzb_with_diagnostics(&xml).map_err(|error| error.to_string())?;
    let report = weaver_nzb::analysis::analyze(&nzb, &diagnostics, now_epoch_secs)
        .with_weaver_version(env!("CARGO_PKG_VERSION"));
    Ok(if json {
        report.to_json()
    } else {
        report.to_text().trim_end().to_string()
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::render;

    const NOW: u64 = 1_790_000_000;

    fn sample_nzb() -> Vec<u8> {
        let mut xml = String::from(
            "<?xml version=\"1.0\"?>\n<nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\n",
        );
        for (index, name) in ["Quiet.Harbor.part1.rar", "Quiet.Harbor.part2.rar"]
            .iter()
            .enumerate()
        {
            xml.push_str(&format!(
                "<file poster=\"someone@cli.invalid\" date=\"1789000000\" \
                 subject=\"[{n}/2] &quot;{name}&quot; yEnc (1/1)\">\
                 <groups><group>alt.binaries.cli</group></groups>\
                 <segments><segment bytes=\"700000\" number=\"1\">p{n}@cli.invalid</segment>\
                 </segments></file>\n",
                n = index + 1,
            ));
        }
        xml.push_str("</nzb>\n");
        xml.into_bytes()
    }

    #[test]
    fn text_report_names_nothing_from_the_nzb() {
        let text = render(sample_nzb(), false, NOW).unwrap();
        assert!(text.contains(env!("CARGO_PKG_VERSION")));
        for piece in ["Quiet", "Harbor", "cli.invalid", "alt.binaries"] {
            assert!(!text.contains(piece), "{piece} leaked into:\n{text}");
        }
    }

    #[test]
    fn gzipped_and_plain_nzbs_give_the_same_json() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&sample_nzb()).unwrap();
        let gzipped = encoder.finish().unwrap();
        let plain = render(sample_nzb(), true, NOW).unwrap();
        assert_eq!(render(gzipped, true, NOW).unwrap(), plain);
        assert!(serde_json::from_str::<serde_json::Value>(&plain).is_ok());
    }

    #[test]
    fn a_file_that_is_not_an_nzb_is_an_error() {
        assert!(render(b"not an nzb".to_vec(), false, NOW).is_err());
    }
}
