use std::fs::File;
use std::io;
use std::path::PathBuf;

use weaver_rar::{ExtractOptions, RarArchive, StaticVolumeProvider};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args().nth(1).ok_or("usage: test-e01 <dir>")?;
    let password = std::env::args().nth(2).ok_or("usage: test-e01 <dir> <password>")?;
    let mode = std::env::args().nth(3).unwrap_or_else(|| "streaming".into());
    let member_idx: usize = std::env::args().nth(4).unwrap_or_else(|| "0".into()).parse()?;

    let mut paths: Vec<PathBuf> = (1..=99)
        .map(|n| {
            PathBuf::from(&dir).join(format!(
                "6OLN1UG33OBhAM9rAfSUdX.part{:03}.rar",
                n
            ))
        })
        .collect();
    paths.sort();

    let first = File::open(&paths[0])?;
    let mut archive = RarArchive::open_with_password(first, &password)?;
    archive.set_password(&password);

    let metadata = archive.metadata();
    let m = &metadata.members[member_idx];
    eprintln!(
        "member[{}]: name={} unpacked={} solid={}",
        member_idx,
        m.name,
        m.unpacked_size.unwrap_or(0),
        archive.is_solid()
    );

    let provider = StaticVolumeProvider::from_ordered(paths.clone());
    let options = ExtractOptions {
        verify: true,
        password: Some(password.clone()),
    };

    let start = std::time::Instant::now();
    let result: Result<(), weaver_rar::RarError> = match mode.as_str() {
        "streaming" => {
            let mut sink = io::sink();
            archive
                .extract_member_streaming(member_idx, &options, &provider, &mut sink)
                .map(|_| ())
        }
        "chunked" => archive
            .extract_member_streaming_chunked(member_idx, &options, &provider, |vol_idx| {
                eprintln!("  writer for vol {}", vol_idx);
                Ok(Box::new(io::sink()) as Box<dyn io::Write>)
            })
            .map(|chunks| {
                eprintln!("  chunks: {:?}", chunks);
            }),
        other => return Err(format!("unknown mode: {other}").into()),
    };

    let elapsed = start.elapsed();
    match result {
        Ok(()) => {
            eprintln!("OK in {:.1?}", elapsed);
        }
        Err(e) => {
            eprintln!("ERR in {:.1?}: {}", elapsed, e);
            std::process::exit(1);
        }
    }

    Ok(())
}
