use std::fs::File;
use std::io;
use std::path::PathBuf;

use weaver_rar::{ExtractOptions, RarArchive, StaticVolumeProvider};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut password: Option<String> = None;
    let mut paths: Vec<PathBuf> = Vec::new();

    while let Some(arg) = args.next() {
        if arg == "--password" {
            let value = args
                .next()
                .ok_or("missing value after --password")?;
            password = Some(value);
            continue;
        }

        paths.push(PathBuf::from(arg));
    }

    if paths.is_empty() {
        return Err("usage: stream_to_sink [--password PASSWORD] <archive> [more-volumes...]".into());
    }

    let first = File::open(&paths[0])?;
    let mut archive = if let Some(ref pwd) = password {
        RarArchive::open_with_password(first, pwd)?
    } else {
        RarArchive::open(first)?
    };

    if let Some(ref pwd) = password {
        archive.set_password(pwd);
    }

    let provider = StaticVolumeProvider::from_ordered(paths.clone());
    let options = ExtractOptions {
        verify: true,
        password,
    };

    let member_count = archive.metadata().members.len();
    if archive.is_solid() {
        for member_index in 0..member_count {
            archive.extract_member_solid_chunked(member_index, &options, |_| {
                Ok(Box::new(io::sink()))
            })?;
        }
    } else {
        let mut sink = io::sink();
        for member_index in 0..member_count {
            archive.extract_member_streaming(member_index, &options, &provider, &mut sink)?;
        }
    }

    Ok(())
}