use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};

use weaver_rar::ArchiveFormat;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(1)
        .ok_or("usage: probe_volumes <dir>")?;
    let password = std::env::args().nth(2);

    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.to_ascii_lowercase().ends_with(".rar"))
        })
        .collect();
    paths.sort();

    for path in paths {
        let mut file = File::open(&path)?;
        print_volume(&path, &mut file, password.as_deref())?;
    }

    Ok(())
}

fn print_volume(
    path: &Path,
    file: &mut File,
    password: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    file.seek(SeekFrom::Start(0))?;
    let format = weaver_rar::signature::read_signature(file)?;

    match format {
        ArchiveFormat::Rar5 => {
            let parsed = weaver_rar::header::parse_all_headers(file, password)?;
            println!(
                "{} format=rar5 vol={:?} files={}",
                path.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("?"),
                parsed.main.as_ref().and_then(|main| main.volume_number),
                parsed.files.len(),
            );
            for (index, entry) in parsed.files.iter().enumerate() {
                println!(
                    "  [{}] split_before={} split_after={} encrypted={} hash_mac={} name={}",
                    index,
                    entry.header.split_before,
                    entry.header.split_after,
                    entry.is_encrypted,
                    entry
                        .file_encryption
                        .as_ref()
                        .is_some_and(|value| value.use_hash_mac),
                    entry.header.name,
                );
            }
        }
        ArchiveFormat::Rar4 => {
            let parsed = weaver_rar::rar4::parse_rar4_headers(file, password)?;
            println!(
                "{} format=rar4 vol={:?} files={}",
                path.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("?"),
                parsed.end.as_ref().and_then(|end| end.volume_number),
                parsed.files.len(),
            );
            for (index, entry) in parsed.files.iter().enumerate() {
                println!(
                    "  [{}] split_before={} split_after={} encrypted={} name={}",
                    index, entry.split_before, entry.split_after, entry.is_encrypted, entry.name,
                );
            }
        }
    }

    Ok(())
}
