// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file's extension reduced to a closed class, for reports that may name
// the kind of a file but never the file.

use serde::Serialize;
use serde::ser::Serializer;

// A file extension from a fixed list, or a shape class for numbered ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileExt(&'static str);

impl FileExt {
    pub const NONE: Self = Self("none");
    pub const OTHER: Self = Self("other");

    pub fn as_str(self) -> &'static str {
        self.0
    }
}

impl Serialize for FileExt {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}

// Extensions a report may name. Anything else is reported as `other`.
const KNOWN_EXTENSIONS: &[&str] = &[
    "rar", "par2", "par3", "7z", "zip", "tar", "gz", "tgz", "bz2", "tbz", "tbz2", "xz", "txz",
    "zst", "zstd", "br", "deflate", "nfo", "sfv", "srr", "srs", "nzb", "txt", "md5", "url", "jpg",
    "jpeg", "png", "gif", "mkv", "mp4", "m4v", "avi", "wmv", "mov", "ts", "m2ts", "vob", "iso",
    "img", "bin", "cue", "flac", "mp3", "m4a", "m4b", "aac", "ogg", "opus", "wav", "epub", "mobi",
    "azw3", "pdf", "cbz", "cbr", "srt", "ass", "sub", "idx", "sup", "exe", "msi", "dmg", "apk",
];

// Extensions that run when opened: a media download carrying one is suspect.
const EXECUTABLE_EXTENSIONS: &[&str] = &[
    "exe", "msi", "scr", "bat", "cmd", "com", "pif", "lnk", "vbs", "vbe", "js", "jse", "wsf",
    "hta", "ps1", "jar", "apk", "dmg", "app", "reg",
];

fn extension(lower: &str) -> Option<&str> {
    lower
        .rsplit_once('.')
        .map(|(_, ext)| ext)
        .filter(|ext| !ext.is_empty())
}

// Classify the extension of a name.
pub fn file_ext(name: &str) -> FileExt {
    let lower = name.to_ascii_lowercase();
    let Some(ext) = extension(&lower) else {
        return FileExt::NONE;
    };
    let bytes = ext.as_bytes();
    if bytes.len() == 3 && bytes.iter().all(u8::is_ascii_digit) {
        return FileExt("NNN");
    }
    if bytes.len() == 3 && bytes[1..].iter().all(u8::is_ascii_digit) {
        match bytes[0] {
            b'r' => return FileExt("rNN"),
            b's' => return FileExt("sNN"),
            _ => {}
        }
    }
    KNOWN_EXTENSIONS
        .iter()
        .find(|known| **known == ext)
        .map_or(FileExt::OTHER, |known| FileExt(known))
}

// Whether a name ends in an extension that runs when opened.
pub fn is_executable_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    extension(&lower).is_some_and(|ext| EXECUTABLE_EXTENSIONS.contains(&ext))
}

#[cfg(test)]
mod tests {
    use super::{file_ext, is_executable_name};

    #[test]
    fn extensions_reduce_to_a_closed_class() {
        assert_eq!(file_ext("A.MKV").as_str(), "mkv");
        assert_eq!(file_ext("a.r05").as_str(), "rNN");
        assert_eq!(file_ext("a.7z.001").as_str(), "NNN");
        assert_eq!(file_ext("a.weird").as_str(), "other");
        assert_eq!(file_ext("abc").as_str(), "none");
        assert_eq!(file_ext("abc.").as_str(), "none");
    }

    #[test]
    fn executables_are_told_by_their_extension() {
        assert!(is_executable_name("setup.EXE"));
        assert!(is_executable_name("play.mkv.scr"));
        assert!(!is_executable_name("film.mkv"));
        assert!(!is_executable_name("exe"));
    }
}
