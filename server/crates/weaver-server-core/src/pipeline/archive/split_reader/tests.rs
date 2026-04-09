use super::*;
use std::io::{Read, Seek, SeekFrom, Write};
use tempfile::NamedTempFile;

fn create_temp_files(contents: &[&[u8]]) -> Vec<NamedTempFile> {
    contents
        .iter()
        .map(|data| {
            let mut f = NamedTempFile::new().unwrap();
            f.write_all(data).unwrap();
            f.flush().unwrap();
            f
        })
        .collect()
}

#[test]
fn read_sequential() {
    let files = create_temp_files(&[b"Hello", b" ", b"World"]);
    let paths: Vec<_> = files.iter().map(|f| f.path()).collect();
    let mut reader = SplitFileReader::open(&paths).unwrap();

    let mut buf = String::new();
    reader.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "Hello World");
}

#[test]
fn seek_across_boundaries() {
    let files = create_temp_files(&[b"AAABBB", b"CCCDDD"]);
    let paths: Vec<_> = files.iter().map(|f| f.path()).collect();
    let mut reader = SplitFileReader::open(&paths).unwrap();

    // Seek into the second file.
    reader.seek(SeekFrom::Start(9)).unwrap();
    let mut buf = [0u8; 3];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"DDD");

    // Seek back to boundary.
    reader.seek(SeekFrom::Start(6)).unwrap();
    let mut buf = [0u8; 6];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"CCCDDD");
}

#[test]
fn seek_from_end() {
    let files = create_temp_files(&[b"AB", b"CD"]);
    let paths: Vec<_> = files.iter().map(|f| f.path()).collect();
    let mut reader = SplitFileReader::open(&paths).unwrap();

    reader.seek(SeekFrom::End(-2)).unwrap();
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"CD");
}

#[test]
fn read_spanning_parts() {
    let files = create_temp_files(&[b"AB", b"CD", b"EF"]);
    let paths: Vec<_> = files.iter().map(|f| f.path()).collect();
    let mut reader = SplitFileReader::open(&paths).unwrap();

    reader.seek(SeekFrom::Start(1)).unwrap();
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"BCDE");
}

#[test]
fn empty_input_errors() {
    let paths: Vec<&Path> = vec![];
    assert!(SplitFileReader::open(&paths).is_err());
}

#[test]
fn single_file() {
    let files = create_temp_files(&[b"single file content"]);
    let paths: Vec<_> = files.iter().map(|f| f.path()).collect();
    let mut reader = SplitFileReader::open(&paths).unwrap();

    let mut buf = String::new();
    reader.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "single file content");
}
